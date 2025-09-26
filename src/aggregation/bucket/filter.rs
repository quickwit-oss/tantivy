use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{CollectorClone, SegmentAggregationCollector};
use crate::query::{Query, QueryParser, RangeQuery, TermQuery, Weight};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::{DocId, SegmentReader, TantivyError, TERMINATED};
use common::bounds::BoundsRange;

/// Filter aggregation creates a single bucket containing documents that match a query.
///
/// This is equivalent to Elasticsearch's filter aggregation and supports the same JSON syntax.
///
/// # Example JSON
/// ```json
/// {
///   "t_shirts": {
///     "filter": { "query_string": "type:t-shirt" },
///     "aggs": {
///       "avg_price": { "avg": { "field": "price" } }
///     }
///   }
/// }
/// ```
///
/// # Result
/// The filter aggregation returns a single bucket with:
/// - `doc_count`: Number of documents matching the filter
/// - Sub-aggregation results computed on the filtered document set
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterAggregation {
    /// The query as raw JSON - will be parsed during collector creation
    #[serde(flatten)]
    pub query: serde_json::Value,
}

impl FilterAggregation {
    /// Create a new filter aggregation with the given query JSON
    pub fn new(query: serde_json::Value) -> Self {
        Self { query }
    }

    /// Parse the stored query JSON into a Tantivy Query object
    ///
    /// This method uses QueryParser to enable advanced features like field boosts,
    /// fuzzy matching, and default fields. For basic parsing without these features,
    /// use the standalone `crate::query::parse_query` function.
    pub fn parse_query(&self, schema: &Schema) -> crate::Result<Box<dyn Query>> {
        // Create a QueryParser with default settings
        // This enables feature inheritance like boosts and fuzzy matching
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(schema.clone(), vec![], tokenizer_manager);

        query_parser
            .parse_json_query(&self.query)
            .map_err(|e| TantivyError::InvalidArgument(e.to_string()))
    }

    /// Parse the stored query JSON into a Tantivy Query object with custom QueryParser
    ///
    /// This method allows using a pre-configured QueryParser with custom settings
    /// like field boosts, fuzzy matching, default fields, etc.
    pub fn parse_query_with_parser(
        &self,
        query_parser: &QueryParser,
    ) -> crate::Result<Box<dyn Query>> {
        query_parser
            .parse_json_query(&self.query)
            .map_err(|e| TantivyError::InvalidArgument(e.to_string()))
    }

    /// Get the fast field names used by this aggregation (none for filter aggregation)
    pub fn get_fast_field_names(&self) -> Vec<&str> {
        // Filter aggregation doesn't use fast fields directly
        vec![]
    }
}

/// Efficient document evaluator for filter queries
/// This avoids running separate query executions and instead evaluates queries per document
pub struct DocumentQueryEvaluator {
    /// The compiled query for evaluation
    query: Box<dyn Query>,
    /// Cached weight for the current segment
    weight: Option<Box<dyn Weight>>,
    /// Schema for field resolution
    schema: Schema,
}

impl DocumentQueryEvaluator {
    /// Create a new document query evaluator
    pub fn new(query: Box<dyn Query>, schema: Schema) -> Self {
        Self {
            query,
            weight: None,
            schema,
        }
    }

    /// Initialize the evaluator for a specific segment
    pub fn initialize_for_segment(&mut self, _segment_reader: &SegmentReader) -> crate::Result<()> {
        use crate::query::EnableScoring;
        self.weight = Some(
            self.query
                .weight(EnableScoring::disabled_from_schema(&self.schema))?,
        );
        Ok(())
    }

    /// Efficiently evaluate if a document matches the filter query
    /// This is the core performance-critical method
    pub fn matches_document(
        &self,
        doc: DocId,
        segment_reader: &SegmentReader,
    ) -> crate::Result<bool> {
        if let Some(weight) = &self.weight {
            // Try fast path optimizations first
            if let Some(result) = self.try_fast_path_evaluation(doc, segment_reader)? {
                return Ok(result);
            }

            // Fall back to full query evaluation
            let mut scorer = weight.scorer(segment_reader, 1.0)?;

            // Advance scorer to the target document
            let mut current_doc = scorer.doc();
            while current_doc < doc {
                current_doc = scorer.advance();
                if current_doc == TERMINATED {
                    return Ok(false);
                }
            }

            Ok(current_doc == doc)
        } else {
            Err(TantivyError::InvalidArgument(
                "DocumentQueryEvaluator not initialized for segment".to_string(),
            ))
        }
    }

    /// Fast path optimizations for common query types
    /// Returns Some(bool) if we can evaluate quickly, None if we need full evaluation
    fn try_fast_path_evaluation(
        &self,
        doc: DocId,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Option<bool>> {
        use crate::query::{AllQuery, RangeQuery, TermQuery};

        // Try to downcast to specific query types for fast evaluation
        if let Some(_all_query) = self.query.downcast_ref::<AllQuery>() {
            // AllQuery matches all documents
            return Ok(Some(true));
        }

        if let Some(term_query) = self.query.downcast_ref::<TermQuery>() {
            return self.evaluate_term_query_fast(term_query, doc, segment_reader);
        }

        if let Some(range_query) = self.query.downcast_ref::<RangeQuery>() {
            return self.evaluate_range_query_fast(range_query, doc, segment_reader);
        }

        // For other query types, use full evaluation
        Ok(None)
    }

    /// Fast evaluation for term queries using fast fields
    fn evaluate_term_query_fast(
        &self,
        term_query: &TermQuery,
        doc: DocId,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Option<bool>> {
        let term = term_query.term();
        let field = term.field();
        let field_entry = self.schema.get_field_entry(field);

        // Only use fast path if field is configured as fast field
        if !field_entry.is_fast() {
            return Ok(None);
        }

        let fast_fields = segment_reader.fast_fields();

        match term.typ() {
            crate::schema::Type::Str => {
                if let Ok(Some(column)) = fast_fields.str(field_entry.name()) {
                    if let Some(term_text) = term.value().as_str() {
                        let mut term_ord_buffer = Vec::new();

                        // Get term ordinals for this document
                        for term_ord in column.term_ords(doc) {
                            term_ord_buffer.clear();
                            if column.ord_to_bytes(term_ord, &mut term_ord_buffer)? {
                                if let Ok(doc_term) = std::str::from_utf8(&term_ord_buffer) {
                                    if doc_term == term_text {
                                        return Ok(Some(true));
                                    }
                                }
                            }
                        }
                        return Ok(Some(false));
                    }
                }
            }
            crate::schema::Type::U64 => {
                if let Ok(column) = fast_fields.u64(field_entry.name()) {
                    let term_value = term.value().as_u64().unwrap();
                    for doc_value in column.values_for_doc(doc) {
                        if doc_value == term_value {
                            return Ok(Some(true));
                        }
                    }
                    return Ok(Some(false));
                }
            }
            crate::schema::Type::I64 => {
                if let Ok(column) = fast_fields.i64(field_entry.name()) {
                    let term_value = term.value().as_i64().unwrap();
                    for doc_value in column.values_for_doc(doc) {
                        if doc_value == term_value {
                            return Ok(Some(true));
                        }
                    }
                    return Ok(Some(false));
                }
            }
            crate::schema::Type::F64 => {
                if let Ok(column) = fast_fields.f64(field_entry.name()) {
                    let term_value = term.value().as_f64().unwrap();
                    for doc_value in column.values_for_doc(doc) {
                        if (doc_value - term_value).abs() < f64::EPSILON {
                            return Ok(Some(true));
                        }
                    }
                    return Ok(Some(false));
                }
            }
            crate::schema::Type::Bool => {
                if let Ok(column) = fast_fields.bool(field_entry.name()) {
                    let term_value = term.value().as_bool().unwrap();
                    for doc_value in column.values_for_doc(doc) {
                        if doc_value == term_value {
                            return Ok(Some(true));
                        }
                    }
                    return Ok(Some(false));
                }
            }
            crate::schema::Type::Date => {
                if let Ok(column) = fast_fields.date(field_entry.name()) {
                    let term_value = term.value().as_date().unwrap();
                    for doc_value in column.values_for_doc(doc) {
                        if doc_value == term_value {
                            return Ok(Some(true));
                        }
                    }
                    return Ok(Some(false));
                }
            }
            _ => {
                // Unsupported type for fast path
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Fast evaluation for range queries using fast fields
    fn evaluate_range_query_fast(
        &self,
        range_query: &RangeQuery,
        doc: DocId,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Option<bool>> {
        let field = range_query.field();
        let field_entry = self.schema.get_field_entry(field);

        // Only use fast path if field is configured as fast field
        if !field_entry.is_fast() {
            return Ok(None);
        }

        let fast_fields = segment_reader.fast_fields();

        match range_query.value_type() {
            crate::schema::Type::U64 => {
                if let Ok(column) = fast_fields.u64(field_entry.name()) {
                    return Ok(Some(self.check_range(range_query, &column, doc)?));
                }
            }
            crate::schema::Type::I64 => {
                if let Ok(column) = fast_fields.i64(field_entry.name()) {
                    return Ok(Some(self.check_range(range_query, &column, doc)?));
                }
            }
            crate::schema::Type::F64 => {
                if let Ok(column) = fast_fields.f64(field_entry.name()) {
                    return Ok(Some(self.check_range(range_query, &column, doc)?));
                }
            }
            crate::schema::Type::Date => {
                if let Ok(column) = fast_fields.date(field_entry.name()) {
                    return Ok(Some(self.check_range(range_query, &column, doc)?));
                }
            }
            _ => {
                // Unsupported type for fast path
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Range checking using BoundsRange
    fn check_range<T>(
        &self,
        range_query: &RangeQuery,
        column: &crate::fastfield::Column<T>,
        doc: DocId,
    ) -> crate::Result<bool>
    where
        T: PartialOrd + Copy + std::fmt::Debug + Send + Sync + 'static,
    {
        // Transform the BoundsRange<Term> to BoundsRange<T> using map_bound
        let term_bounds = range_query.bounds_range();

        // Check each document value against the range
        for doc_value in column.values_for_doc(doc) {
            if self.check_value_against_term_bounds(doc_value, term_bounds)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if a value matches the term bounds - type-specific implementations
    fn check_value_against_term_bounds<T>(
        &self,
        value: T,
        bounds: &BoundsRange<crate::schema::Term>,
    ) -> crate::Result<bool>
    where
        T: PartialOrd + Copy + 'static,
    {
        use std::any::TypeId;

        // Handle each type specifically since we can't make this truly generic
        let type_id = TypeId::of::<T>();

        if type_id == TypeId::of::<u64>() {
            let value = unsafe { *((&value) as *const T as *const u64) };
            self.check_bounds_generic(value, bounds, |term| term.value().as_u64())
        } else if type_id == TypeId::of::<i64>() {
            let value = unsafe { *((&value) as *const T as *const i64) };
            self.check_bounds_generic(value, bounds, |term| term.value().as_i64())
        } else if type_id == TypeId::of::<f64>() {
            let value = unsafe { *((&value) as *const T as *const f64) };
            self.check_bounds_generic(value, bounds, |term| term.value().as_f64())
        } else if type_id == TypeId::of::<crate::DateTime>() {
            let value = unsafe { *((&value) as *const T as *const crate::DateTime) };
            self.check_bounds_generic(value, bounds, |term| term.value().as_date())
        } else {
            Err(TantivyError::InvalidArgument(
                "Unsupported type for range checking".to_string(),
            ))
        }
    }

    /// Generic bounds checking logic
    fn check_bounds_generic<T, F>(
        &self,
        value: T,
        bounds: &BoundsRange<crate::schema::Term>,
        extractor: F,
    ) -> crate::Result<bool>
    where
        T: PartialOrd,
        F: Fn(&crate::schema::Term) -> Option<T>,
    {
        use std::ops::Bound;

        // Check lower bound
        let lower_ok = match &bounds.lower_bound {
            Bound::Included(term) => {
                let bound_value = extractor(term).ok_or_else(|| {
                    TantivyError::InvalidArgument("Failed to extract bound value".to_string())
                })?;
                value >= bound_value
            }
            Bound::Excluded(term) => {
                let bound_value = extractor(term).ok_or_else(|| {
                    TantivyError::InvalidArgument("Failed to extract bound value".to_string())
                })?;
                value > bound_value
            }
            Bound::Unbounded => true,
        };

        // Check upper bound
        let upper_ok = match &bounds.upper_bound {
            Bound::Included(term) => {
                let bound_value = extractor(term).ok_or_else(|| {
                    TantivyError::InvalidArgument("Failed to extract bound value".to_string())
                })?;
                value <= bound_value
            }
            Bound::Excluded(term) => {
                let bound_value = extractor(term).ok_or_else(|| {
                    TantivyError::InvalidArgument("Failed to extract bound value".to_string())
                })?;
                value < bound_value
            }
            Bound::Unbounded => true,
        };

        Ok(lower_ok && upper_ok)
    }
}

impl Debug for DocumentQueryEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentQueryEvaluator")
            .field("has_weight", &self.weight.is_some())
            .finish()
    }
}

/// Segment collector for filter aggregation
pub struct FilterSegmentCollector {
    /// Document evaluator for the filter query
    evaluator: DocumentQueryEvaluator,
    /// Document count in this bucket
    doc_count: u64,
    /// Sub-aggregation collectors
    sub_aggregations: Option<Box<dyn SegmentAggregationCollector>>,
    /// Segment reader reference for document evaluation
    segment_reader: SegmentReader,
    /// Accessor index for this filter aggregation
    accessor_idx: usize,
}

impl FilterSegmentCollector {
    /// Create a new filter segment collector following the same pattern as other bucket aggregations
    pub(crate) fn from_req_and_validate(
        filter_req: &FilterAggregation,
        sub_aggregations: &mut AggregationsWithAccessor,
        segment_reader: &SegmentReader,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        let schema = segment_reader.schema();
        let query = filter_req.parse_query(&schema)?;

        let mut evaluator = DocumentQueryEvaluator::new(query, schema.clone());
        evaluator.initialize_for_segment(segment_reader)?;

        // Follow the same pattern as terms aggregation
        let has_sub_aggregations = !sub_aggregations.is_empty();
        let sub_agg_collector = if has_sub_aggregations {
            use crate::aggregation::segment_agg_result::build_segment_agg_collector_with_reader;
            // Use the same sub_aggregations structure that will be used at runtime
            // This ensures that the accessor indices match between build-time and runtime
            // Pass the segment_reader to ensure nested filter aggregations also get access
            let sub_aggregation =
                build_segment_agg_collector_with_reader(sub_aggregations, Some(segment_reader))?;
            Some(sub_aggregation)
        } else {
            None
        };

        Ok(FilterSegmentCollector {
            evaluator,
            doc_count: 0,
            sub_aggregations: sub_agg_collector,
            segment_reader: segment_reader.clone(),
            accessor_idx,
        })
    }
}

impl Debug for FilterSegmentCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterSegmentCollector")
            .field("doc_count", &self.doc_count)
            .field("has_sub_aggs", &self.sub_aggregations.is_some())
            .field("evaluator", &self.evaluator)
            .finish()
    }
}

impl CollectorClone for FilterSegmentCollector {
    fn clone_box(&self) -> Box<dyn SegmentAggregationCollector> {
        // For now, panic - this needs proper implementation with weight recreation
        panic!("FilterSegmentCollector cloning not yet implemented - requires weight recreation")
    }
}

impl SegmentAggregationCollector for FilterSegmentCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let mut sub_results = IntermediateAggregationResults::default();

        if let Some(sub_aggs) = self.sub_aggregations {
            // Use the same pattern as collect: pass the sub-aggregation accessor structure
            let bucket_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];
            sub_aggs.add_intermediate_aggregation_result(
                &bucket_accessor.sub_aggregation,
                &mut sub_results,
            )?;
        }

        // Create the proper filter bucket result
        let filter_bucket_result = IntermediateBucketResult::Filter {
            doc_count: self.doc_count,
            sub_aggregations: sub_results,
        };

        // Get the name of this filter aggregation
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        results.push(
            name,
            IntermediateAggregationResult::Bucket(filter_bucket_result),
        )?;

        Ok(())
    }

    fn collect(
        &mut self,
        doc: DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        // This is the core efficiency: evaluate filter on document already matched by main query
        if self.evaluator.matches_document(doc, &self.segment_reader)? {
            self.doc_count += 1;

            // If we have sub-aggregations, collect on them for this filtered document
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];
                sub_aggs.collect(doc, &mut bucket_agg_accessor.sub_aggregation)?;
            }
        }
        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        // Batch processing for better performance
        for &doc in docs {
            self.collect(doc, agg_with_accessor)?;
        }
        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        if let Some(ref mut sub_aggs) = self.sub_aggregations {
            let accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;
            sub_aggs.flush(accessor)?;
        }
        Ok(())
    }
}

/// Intermediate result for filter aggregation
#[derive(Debug, Clone, PartialEq)]
pub struct IntermediateFilterBucketResult {
    /// Document count in this bucket
    pub doc_count: u64,
    /// Sub-aggregation results
    pub sub_aggregations: IntermediateAggregationResults,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryParser;
    use crate::schema::{Schema, FAST, INDEXED, TEXT};
    use crate::tokenizer::TokenizerManager;
    use serde_json::json;

    #[test]
    fn test_filter_aggregation_serde() {
        let filter_agg = FilterAggregation::new(json!({
            "term": { "category": "electronics" }
        }));

        let serialized = serde_json::to_string(&filter_agg).unwrap();
        let deserialized: FilterAggregation = serde_json::from_str(&serialized).unwrap();

        assert_eq!(filter_agg, deserialized);
    }

    #[test]
    fn test_parse_query_string() {
        use crate::schema::INDEXED;
        let mut schema_builder = Schema::builder();
        let category_field = schema_builder.add_text_field("category", TEXT); // TEXT fields are indexed by default
        let price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
        let schema = schema_builder.build();

        // Create QueryParser for testing
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(
            schema.clone(),
            vec![category_field, price_field],
            tokenizer_manager,
        );

        // Test string query
        let query_json = json!({ "query_string": "category:electronics" });
        let _query = query_parser.parse_json_query(&query_json).unwrap();

        // Test numeric query
        let query_json = json!({ "query_string": "price:100" });
        let _query = query_parser.parse_json_query(&query_json).unwrap();
    }

    #[test]
    fn test_parse_range_query() {
        use crate::schema::INDEXED;
        let mut schema_builder = Schema::builder();
        let price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
        let schema = schema_builder.build();

        // Create QueryParser for testing
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(schema.clone(), vec![price_field], tokenizer_manager);

        let query_json = json!({ "query_string": "price:[10 TO 100}" });
        let _query = query_parser.parse_json_query(&query_json).unwrap();
    }

    #[test]
    fn test_parse_boolean_query() {
        let mut schema_builder = Schema::builder();
        let category_field = schema_builder.add_text_field("category", TEXT);
        let schema = schema_builder.build();

        // Create QueryParser for testing
        let tokenizer_manager = TokenizerManager::default();
        let query_parser =
            QueryParser::new(schema.clone(), vec![category_field], tokenizer_manager);

        // Test boolean query with string clauses
        let query_json = json!({
            "bool": {
                "must": ["category:electronics"],
                "must_not": ["category:discontinued"]
            }
        });
        let _query = query_parser.parse_json_query(&query_json).unwrap();

        // Test match_all query using string syntax
        let query_json = json!("*");
        let _query = query_parser.parse_json_query(&query_json).unwrap();

        // Test invalid query type
        let query_json = json!({ "invalid_query": {} });
        let result = query_parser.parse_json_query(&query_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_document_query_evaluator() {
        use crate::query::TermQuery;
        use crate::schema::{IndexRecordOption, Term};
        use crate::schema::{Schema, TEXT};

        let mut schema_builder = Schema::builder();
        let category_field = schema_builder.add_text_field("category", TEXT);
        let schema = schema_builder.build();

        // Create a simple term query
        let term = Term::from_field_text(category_field, "electronics");
        let query = Box::new(TermQuery::new(term, IndexRecordOption::Basic));

        // Create the document evaluator
        let evaluator = DocumentQueryEvaluator::new(query, schema);

        // Verify it was created successfully
        assert!(evaluator.weight.is_none()); // Should be None until initialized
    }

    #[test]
    fn test_parse_complex_bool_query() {
        use crate::schema::{Schema, FAST, TEXT};

        let mut schema_builder = Schema::builder();
        let _category_field = schema_builder.add_text_field("category", TEXT);
        let _price_field = schema_builder.add_u64_field("price", FAST);
        let schema = schema_builder.build();

        // Test complex bool query using string queries
        let query_json = json!({
            "bool": {
                "must": [
                    "category:electronics"
                ],
                "should": [
                    "price:[100 TO 500}"
                ],
                "must_not": [
                    "category:discontinued"
                ]
            }
        });

        // Create QueryParser for testing
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(
            schema.clone(),
            vec![_category_field, _price_field],
            tokenizer_manager,
        );

        let _query = query_parser.parse_json_query(&query_json).unwrap();
        // Should parse successfully without errors
    }
}
