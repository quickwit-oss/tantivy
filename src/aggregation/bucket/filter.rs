use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector_with_reader, CollectorClone, SegmentAggregationCollector,
};
use crate::query::{EnableScoring, Query, QueryParser, Weight};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::{DocId, SegmentReader, TantivyError, TERMINATED};

/// Filter aggregation creates a single bucket containing documents that match a query.
///
/// This is equivalent to Elasticsearch's filter aggregation and supports the same JSON syntax.
///
/// # Example JSON
/// ```json
/// {
///   "t_shirts": {
///     "filter": { "term": { "type": "t-shirt" } },
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
    pub fn new(query: Box<dyn Query>, schema: Schema) -> Self {
        Self {
            query,
            weight: None,
            schema,
        }
    }

    /// Initialize the evaluator for a specific segment
    pub fn initialize_for_segment(&mut self, segment_reader: &SegmentReader) -> crate::Result<()> {
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
        _doc: DocId,
        _segment_reader: &SegmentReader,
    ) -> crate::Result<Option<bool>> {
        // TODO: Implement fast paths for:
        // - Term queries using fast fields
        // - Range queries using fast fields
        // - Simple boolean combinations

        // For now, always use full evaluation
        Ok(None)
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
    pub fn from_req_and_validate(
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

    /// Create a new filter segment collector (deprecated - use from_req_and_validate)
    pub fn new(
        query: Box<dyn Query>,
        schema: Schema,
        segment_reader: &SegmentReader,
        sub_aggregations: Option<Box<dyn SegmentAggregationCollector>>,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        let mut evaluator = DocumentQueryEvaluator::new(query, schema);
        evaluator.initialize_for_segment(segment_reader)?;

        Ok(FilterSegmentCollector {
            evaluator,
            doc_count: 0,
            sub_aggregations,
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
    pub doc_count: u64,
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
    fn test_filter_aggregation_json_parsing() {
        use crate::aggregation::agg_req::{AggregationVariants, Aggregations};

        // Test basic filter aggregation parsing
        let agg_request = json!({
            "t_shirts": {
                "filter": { "term": { "type": "t-shirt" } },
                "aggs": {
                    "avg_price": { "avg": { "field": "price" } }
                }
            }
        });

        // This should parse successfully now that we've added Filter to AggregationVariants
        let aggregations: Aggregations = serde_json::from_value(agg_request).unwrap();

        assert_eq!(aggregations.len(), 1);

        // Verify the structure
        let t_shirts_agg = aggregations.get("t_shirts").unwrap();

        // Check if it's a filter aggregation
        match &t_shirts_agg.agg {
            AggregationVariants::Filter(filter_agg) => {
                // Verify the filter query is stored correctly
                assert!(filter_agg.query.is_object());
                let query_obj = filter_agg.query.as_object().unwrap();
                assert!(query_obj.contains_key("term"));
            }
            _ => panic!("Expected filter aggregation, got something else"),
        }

        // Check sub-aggregations
        assert!(t_shirts_agg.sub_aggregation.contains_key("avg_price"));

        // Verify the sub-aggregation is an average aggregation
        let avg_agg = t_shirts_agg.sub_aggregation.get("avg_price").unwrap();
        match &avg_agg.agg {
            AggregationVariants::Average(_) => {
                // Expected
            }
            _ => panic!("Expected average aggregation in sub-aggregation"),
        }
    }

    #[test]
    fn test_document_query_evaluator() {
        use crate::query::TermQuery;
        use crate::schema::{IndexRecordOption, Term};
        use crate::schema::{Schema, FAST, TEXT};

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
