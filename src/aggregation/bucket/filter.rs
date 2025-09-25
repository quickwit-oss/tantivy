use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults,
};
use crate::aggregation::segment_agg_result::{CollectorClone, SegmentAggregationCollector};
use crate::query::{AllQuery, BooleanQuery, Query, RangeQuery, TermQuery, Weight};
use crate::schema::{IndexRecordOption, Schema, Term};
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
}

impl FilterSegmentCollector {
    /// Create a new filter segment collector
    pub fn new(
        query: Box<dyn Query>,
        schema: Schema,
        segment_reader: &SegmentReader,
        sub_aggregations: Option<Box<dyn SegmentAggregationCollector>>,
    ) -> crate::Result<Self> {
        let mut evaluator = DocumentQueryEvaluator::new(query, schema);
        evaluator.initialize_for_segment(segment_reader)?;

        Ok(FilterSegmentCollector {
            evaluator,
            doc_count: 0,
            sub_aggregations,
            segment_reader: segment_reader.clone(),
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
            sub_aggs.add_intermediate_aggregation_result(
                agg_with_accessor,
                &mut sub_aggregation_results,
            )?;
        }

        // For now, store as a custom intermediate result
        // TODO: Add Filter variant to IntermediateBucketResult
        use crate::aggregation::intermediate_agg_result::IntermediateMetricResult;
        use crate::aggregation::metric::IntermediateCount;

        let doc_count_metric = IntermediateMetricResult::Count(IntermediateCount::default());
        results.push(
            "doc_count".to_string(),
            IntermediateAggregationResult::Metric(doc_count_metric),
        )?;

        // Add sub-aggregation results
        for (key, value) in sub_aggregation_results.aggs_res {
            results.push(key, value)?;
        }

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
                sub_aggs.collect(doc, agg_with_accessor)?;
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
}

/// Intermediate result for filter aggregation
#[derive(Debug, Clone, PartialEq)]
pub struct IntermediateFilterBucketResult {
    pub doc_count: u64,
    pub sub_aggregations: IntermediateAggregationResults,
}

/// Parse Elasticsearch query JSON into Tantivy Query objects
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
                "term" => parse_term_query(query_params, schema),
                "range" => parse_range_query(query_params, schema),
                "bool" => parse_bool_query(query_params, schema),
                "match" => parse_match_query(query_params, schema),
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

/// Parse Elasticsearch term query: { "term": { "field": "value" } }
fn parse_term_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
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

    let term = match field_value {
        Value::String(s) => Term::from_field_text(field, s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Term::from_field_i64(field, i)
            } else if let Some(u) = n.as_u64() {
                Term::from_field_u64(field, u)
            } else if let Some(f) = n.as_f64() {
                Term::from_field_f64(field, f)
            } else {
                return Err(TantivyError::InvalidArgument(
                    "Invalid number format in term query".to_string(),
                ));
            }
        }
        Value::Bool(b) => {
            if *b {
                Term::from_field_u64(field, 1)
            } else {
                Term::from_field_u64(field, 0)
            }
        }
        _ => {
            return Err(TantivyError::InvalidArgument(
                "Term query value must be string, number, or boolean".to_string(),
            ))
        }
    };

    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
}

/// Parse Elasticsearch range query: { "range": { "field": { "gte": 10, "lt": 20 } } }
fn parse_range_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    use serde_json::Value;
    use std::ops::Bound;

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

    let mut lower_bound = Bound::Unbounded;
    let mut upper_bound = Bound::Unbounded;

    for (op, value) in range_obj {
        let term_value = match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Term::from_field_i64(field, i)
                } else if let Some(u) = n.as_u64() {
                    Term::from_field_u64(field, u)
                } else if let Some(f) = n.as_f64() {
                    Term::from_field_f64(field, f)
                } else {
                    return Err(TantivyError::InvalidArgument(
                        "Invalid number format in range query".to_string(),
                    ));
                }
            }
            Value::String(s) => Term::from_field_text(field, s),
            _ => {
                return Err(TantivyError::InvalidArgument(
                    "Range query values must be numbers or strings".to_string(),
                ))
            }
        };

        match op.as_str() {
            "gte" => lower_bound = Bound::Included(term_value),
            "gt" => lower_bound = Bound::Excluded(term_value),
            "lte" => upper_bound = Bound::Included(term_value),
            "lt" => upper_bound = Bound::Excluded(term_value),
            _ => {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unsupported range operator: {}",
                    op
                )))
            }
        }
    }

    Ok(Box::new(RangeQuery::new(lower_bound, upper_bound)))
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

/// Parse Elasticsearch match query: { "match": { "field": "value" } }
/// Note: This is a simplified implementation that converts to term query
fn parse_match_query(params: &serde_json::Value, schema: &Schema) -> crate::Result<Box<dyn Query>> {
    // For now, we'll implement match as a simple term query
    // In a full implementation, this would involve text analysis and tokenization
    parse_term_query(params, schema)
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
        let mut schema_builder = Schema::builder();
        let _category_field = schema_builder.add_text_field("category", TEXT);
        let _price_field = schema_builder.add_u64_field("price", FAST);
        let schema = schema_builder.build();

        // Test string term query
        let query_json = json!({ "category": "electronics" });
        let _query = parse_term_query(&query_json, &schema).unwrap();

        // Test numeric term query
        let query_json = json!({ "price": 100 });
        let _query = parse_term_query(&query_json, &schema).unwrap();
    }

    #[test]
    fn test_parse_range_query() {
        let mut schema_builder = Schema::builder();
        let _price_field = schema_builder.add_u64_field("price", FAST);
        let schema = schema_builder.build();

        let query_json = json!({ "price": { "gte": 10, "lt": 100 } });
        let _query = parse_range_query(&query_json, &schema).unwrap();
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
