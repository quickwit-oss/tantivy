use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{CollectorClone, SegmentAggregationCollector};
use crate::query::{AllQuery, BooleanQuery, Query, Weight};
use crate::schema::Schema;
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
    pub fn parse_query(&self, schema: &Schema) -> crate::Result<Box<dyn Query>> {
        parse_elasticsearch_query(&self.query, schema)
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
            .field("has_sub_aggregations", &self.sub_aggregations.is_some())
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
        let mut sub_aggregation_results = IntermediateAggregationResults::default();

        if let Some(sub_aggs) = self.sub_aggregations {
            // Use the same pattern as collect: pass the sub-aggregation accessor structure
            let bucket_agg_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];
            sub_aggs.add_intermediate_aggregation_result(
                &bucket_agg_accessor.sub_aggregation,
                &mut sub_aggregation_results,
            )?;
        }

        // Create the proper filter bucket result
        let filter_bucket_result = IntermediateBucketResult::Filter {
            doc_count: self.doc_count,
            sub_aggregations: sub_aggregation_results,
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
        if let Some(sub_aggs) = &mut self.sub_aggregations {
            let sub_aggregation_accessor =
                &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;
            sub_aggs.flush(sub_aggregation_accessor)?;
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

/// Parse Elasticsearch query JSON into Tantivy Query objects using Tantivy's QueryParser
pub fn parse_elasticsearch_query(
    query_json: &serde_json::Value,
    schema: &Schema,
) -> crate::Result<Box<dyn Query>> {
    use serde_json::Value;

    match query_json {
        Value::Object(obj) => {
            if obj.is_empty() {
                return Err(TantivyError::InvalidArgument(
                    "Query object cannot be empty".to_string(),
                ));
            }

            if obj.len() > 1 {
                return Err(TantivyError::InvalidArgument(
                    "Query object should contain exactly one query type".to_string(),
                ));
            }

            let (query_type, query_params) = obj.iter().next().unwrap();
            match query_type.as_str() {
                "term" => parse_term_query_with_tantivy_parser(query_params, schema),
                "range" => parse_range_query_with_tantivy_parser(query_params, schema),
                "bool" => parse_bool_query(query_params, schema),
                "match" => parse_match_query_with_tantivy_parser(query_params, schema),
                "match_all" => Ok(Box::new(AllQuery)),
                _ => Err(TantivyError::InvalidArgument(format!(
                    "Unsupported query type: {}",
                    query_type
                ))),
            }
        }
        _ => Err(TantivyError::InvalidArgument(
            "Query must be a JSON object".to_string(),
        )),
    }
}

/// Parse Elasticsearch term query using Tantivy's QueryParser: { "term": { "field": "value" } }
fn parse_term_query_with_tantivy_parser(
    params: &serde_json::Value,
    schema: &Schema,
) -> crate::Result<Box<dyn Query>> {
    use crate::query::QueryParser;
    use crate::tokenizer::TokenizerManager;
    use serde_json::Value;

    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Term query parameters must be an object".to_string())
    })?;

    if obj.len() != 1 {
        return Err(TantivyError::InvalidArgument(
            "Term query must specify exactly one field".to_string(),
        ));
    }

    let (field_name, field_value) = obj.iter().next().unwrap();
    let field = schema.get_field(field_name).map_err(|_| {
        TantivyError::InvalidArgument(format!("Field '{}' not found in schema", field_name))
    })?;

    // Convert the JSON value to a string for parsing
    let value_str = match field_value {
        Value::String(s) => {
            if s.is_empty() {
                // Handle empty string case - use quotes to make it a valid query
                "\"\"".to_string()
            } else if s.trim().is_empty() {
                // Handle whitespace-only strings - use quotes to preserve them
                format!("\"{}\"", s)
            } else if s.contains(':') || s.contains('-') || s.contains('T') {
                // Handle date/time strings and other special formats - use quotes
                format!("\"{}\"", s)
            } else {
                s.clone()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => {
            return Err(TantivyError::InvalidArgument(
                "Term query value must be string, number, or boolean".to_string(),
            ))
        }
    };

    // Use Tantivy's QueryParser to properly handle type conversion
    let tokenizer_manager = TokenizerManager::default();
    let query_parser = QueryParser::new(schema.clone(), vec![field], tokenizer_manager);

    // Create a query string in the format "field:value"
    let query_str = format!("{}:{}", field_name, value_str);

    query_parser
        .parse_query(&query_str)
        .map_err(|e| TantivyError::InvalidArgument(format!("Failed to parse term query: {}", e)))
}

/// Parse Elasticsearch range query using Tantivy's QueryParser: { "range": { "field": { "gte": 10, "lt": 20 } } }
fn parse_range_query_with_tantivy_parser(
    params: &serde_json::Value,
    schema: &Schema,
) -> crate::Result<Box<dyn Query>> {
    use crate::query::QueryParser;
    use crate::tokenizer::TokenizerManager;
    use serde_json::Value;

    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Range query parameters must be an object".to_string())
    })?;

    if obj.len() != 1 {
        return Err(TantivyError::InvalidArgument(
            "Range query must specify exactly one field".to_string(),
        ));
    }

    let (field_name, range_params) = obj.iter().next().unwrap();
    let field = schema.get_field(field_name).map_err(|_| {
        TantivyError::InvalidArgument(format!("Field '{}' not found in schema", field_name))
    })?;

    let range_obj = range_params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Range query field parameters must be an object".to_string())
    })?;

    // Build a range query string in Tantivy's format
    let mut lower_bound = None;
    let mut upper_bound = None;
    let mut lower_inclusive = true;
    let mut upper_inclusive = true;

    for (bound_type, bound_value) in range_obj {
        let value_str = match bound_value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => {
                return Err(TantivyError::InvalidArgument(
                    "Range query value must be string, number, or boolean".to_string(),
                ))
            }
        };

        match bound_type.as_str() {
            "gte" => {
                lower_bound = Some(value_str);
                lower_inclusive = true;
            }
            "gt" => {
                lower_bound = Some(value_str);
                lower_inclusive = false;
            }
            "lte" => {
                upper_bound = Some(value_str);
                upper_inclusive = true;
            }
            "lt" => {
                upper_bound = Some(value_str);
                upper_inclusive = false;
            }
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported range bound: {}",
                    bound_type
                )))
            }
        }
    }

    if lower_bound.is_none() && upper_bound.is_none() {
        return Err(TantivyError::InvalidArgument(
            "Range query must specify at least one bound".to_string(),
        ));
    }

    // Use Tantivy's QueryParser to properly handle type conversion
    let tokenizer_manager = TokenizerManager::default();
    let query_parser = QueryParser::new(schema.clone(), vec![field], tokenizer_manager);

    // Create a range query string in Tantivy's format: "field:[lower TO upper]"
    let lower_str = lower_bound.as_deref().unwrap_or("*");
    let upper_str = upper_bound.as_deref().unwrap_or("*");
    let left_bracket = if lower_inclusive { "[" } else { "{" };
    let right_bracket = if upper_inclusive { "]" } else { "}" };

    let query_str = format!(
        "{}:{}{} TO {}{}",
        field_name, left_bracket, lower_str, upper_str, right_bracket
    );

    query_parser
        .parse_query(&query_str)
        .map_err(|e| TantivyError::InvalidArgument(format!("Failed to parse range query: {}", e)))
}

/// Parse Elasticsearch bool query: { "bool": { "must": [...], "should": [...], "must_not": [...] } }
fn parse_bool_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    use crate::query::Occur;
    use serde_json::Value;

    let obj = params.as_object().ok_or_else(|| {
        TantivyError::InvalidArgument("Bool query parameters must be an object".to_string())
    })?;

    let mut subqueries = Vec::new();

    for (clause_type, clause_queries) in obj {
        let occur = match clause_type.as_str() {
            "must" => Occur::Must,
            "should" => Occur::Should,
            "must_not" => Occur::MustNot,
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported bool query clause: {}",
                    clause_type
                )))
            }
        };

        let queries_array = clause_queries.as_array().ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "Bool query '{}' clause must be an array",
                clause_type
            ))
        })?;

        for query_json in queries_array {
            let query = parse_elasticsearch_query(query_json, schema)?;
            subqueries.push((occur, query));
        }
    }

    Ok(Box::new(BooleanQuery::new(subqueries)))
}

/// Parse Elasticsearch match query using Tantivy's QueryParser: { "match": { "field": "value" } }
/// Note: This implementation uses Tantivy's QueryParser which handles text analysis and tokenization
fn parse_match_query_with_tantivy_parser(
    params: &serde_json::Value,
    schema: &Schema,
) -> crate::Result<Box<dyn Query>> {
    // For match queries, we can use the same logic as term queries since QueryParser handles tokenization
    parse_term_query_with_tantivy_parser(params, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, FAST, TEXT};
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
    fn test_parse_term_query() {
        use crate::schema::INDEXED;
        let mut schema_builder = Schema::builder();
        let _category_field = schema_builder.add_text_field("category", TEXT); // TEXT fields are indexed by default
        let _price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
        let schema = schema_builder.build();

        // Test string term query
        let query_json = json!({ "category": "electronics" });
        let _query = parse_term_query_with_tantivy_parser(&query_json, &schema).unwrap();

        // Test numeric term query
        let query_json = json!({ "price": 100 });
        let _query = parse_term_query_with_tantivy_parser(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_parse_range_query() {
        use crate::schema::INDEXED;
        let mut schema_builder = Schema::builder();
        let _price_field = schema_builder.add_u64_field("price", FAST | INDEXED);
        let schema = schema_builder.build();

        let query_json = json!({ "price": { "gte": 10, "lt": 100 } });
        let _query = parse_range_query_with_tantivy_parser(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_parse_elasticsearch_query() {
        let mut schema_builder = Schema::builder();
        let _category_field = schema_builder.add_text_field("category", TEXT);
        let schema = schema_builder.build();

        // Test term query
        let query_json = json!({ "term": { "category": "electronics" } });
        let _query = parse_elasticsearch_query(&query_json, &schema).unwrap();

        // Test match_all query
        let query_json = json!({ "match_all": {} });
        let _query = parse_elasticsearch_query(&query_json, &schema).unwrap();

        // Test invalid query type
        let query_json = json!({ "invalid_query": {} });
        let result = parse_elasticsearch_query(&query_json, &schema);
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

        // Test complex bool query
        let query_json = json!({
            "bool": {
                "must": [
                    { "term": { "category": "electronics" } }
                ],
                "should": [
                    { "range": { "price": { "gte": 100, "lt": 500 } } }
                ],
                "must_not": [
                    { "term": { "category": "discontinued" } }
                ]
            }
        });

        let _query = parse_elasticsearch_query(&query_json, &schema).unwrap();
        // Should parse successfully without errors
    }
}
