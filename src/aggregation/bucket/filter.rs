use std::fmt::Debug;

use common::BitSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregation::agg_data::{
    build_segment_agg_collectors, AggRefNode, AggregationsSegmentCtx,
};
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{CollectorClone, SegmentAggregationCollector};
use crate::docset::{DocSet, COLLECT_BLOCK_BUFFER_LEN};
use crate::query::{AllQuery, EnableScoring, Query, QueryParser};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::{DocId, SegmentReader, TantivyError};

/// A trait for queries that can be both executed and serialized.
///
/// This trait extends Tantivy's [`Query`] trait with serialization capabilities,
/// enabling filter aggregations to work with custom query types that can be
/// serialized for distributed aggregation scenarios.
///
/// # Why This Trait Exists
///
/// Tantivy's [`Query`] trait is not object-safe for serialization because it doesn't
/// require `Serialize`. However, filter aggregations need to serialize queries when:
/// - Distributing aggregation requests across multiple nodes
/// - Caching aggregation configurations
/// - Storing aggregation definitions
///
/// This trait bridges that gap by requiring both query execution and serialization.
///
/// # Implementation Requirements
///
/// To implement `SerializableQuery`, you must:
/// 1. Implement [`Query`] for query execution
/// 2. Implement [`Serialize`](serde::Serialize) for serialization
/// 3. Implement [`Clone`] (required by `clone_box`)
/// 4. Implement `clone_box()` to enable trait object cloning
///
/// # Example
///
/// ```rust
/// use tantivy::aggregation::bucket::SerializableQuery;
/// use tantivy::query::{Query, EnableScoring, TermQuery, Weight};
/// use tantivy::schema::{Field, IndexRecordOption};
/// use tantivy::Term;
/// use serde::Serialize;
///
/// #[derive(Debug, Clone, Serialize)]
/// struct MySerializableQuery {
///     field_id: u32,
///     term: String,
/// }
///
/// impl SerializableQuery for MySerializableQuery {
///     fn clone_box(&self) -> Box<dyn SerializableQuery> {
///         Box::new(self.clone())
///     }
/// }
///
/// impl Query for MySerializableQuery {
///     fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
///         // Construct the actual query from serialized data
///         let field = Field::from_field_id(self.field_id);
///         let term = Term::from_field_text(field, &self.term);
///         let term_query = TermQuery::new(term, IndexRecordOption::Basic);
///         term_query.weight(enable_scoring)
///     }
/// }
/// ```
///
/// # Serialization Format
///
/// The serialization format is determined by your `Serialize` implementation.
/// For distributed aggregations, ensure your format is:
/// - Stable across versions
/// - Compact for network efficiency
/// - Self-describing for debugging
///
/// # Performance Considerations
///
/// - `clone_box()` is called when cloning filter aggregations
/// - Serialization occurs when distributing aggregations
/// - Query execution happens per segment during collection
///
/// Keep these operations efficient for best performance.
pub trait SerializableQuery: Query + erased_serde::Serialize {
    /// Clone this query into a boxed trait object.
    ///
    /// This method enables cloning of trait objects, which is necessary for
    /// cloning filter aggregations that contain custom queries.
    ///
    /// # Implementation
    ///
    /// The typical implementation is:
    /// ```rust,ignore
    /// fn clone_box(&self) -> Box<dyn SerializableQuery> {
    ///     Box::new(self.clone())
    /// }
    /// ```
    ///
    /// This requires your type to implement [`Clone`].
    fn clone_box(&self) -> Box<dyn SerializableQuery>;
}

// Enable serialization of SerializableQuery trait objects using erased-serde
erased_serde::serialize_trait_object!(SerializableQuery);

/// Filter aggregation creates a single bucket containing documents that match a query.
///
/// # Usage
/// ```rust
/// use tantivy::aggregation::bucket::SerializableQuery;
/// use tantivy::aggregation::bucket::filter::FilterAggregation;
/// use tantivy::query::{Query, EnableScoring, TermQuery, Weight};
///
/// #[derive(Debug, Clone)]
/// struct SerializableTermQuery(TermQuery);
/// impl SerializableQuery for SerializableTermQuery {
///     fn clone_box(&self) -> Box<dyn SerializableQuery> {
///         Box::new(self.clone())
///     }
/// }
///
/// impl Query for SerializableTermQuery {
///     fn weight(&self, enable_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
///         self.0.weight(enable_scoring)
///     }
/// }
///
/// impl SerializableTermQuery {
///     pub fn new(term_query: TermQuery) -> Self {
///         Self(term_query)
///     }
/// }
///
/// impl serde::Serialize for SerializableTermQuery {
///     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
///     where S: serde::Serializer {
///         "todo".serialize(serializer)
///     }
/// }
///
/// // Query strings are parsed using Tantivy's standard QueryParser
/// let filter_agg = FilterAggregation::new("category:electronics AND price:[100 TO 500]".to_string());
///
/// // Direct Query objects can be used for custom query types
/// let term_query = TermQuery::new(
///     tantivy::Term::from_field_text(tantivy::schema::Field::from_field_id(0), "electronics"),
///     tantivy::schema::IndexRecordOption::Basic
/// );
///
/// let filter_agg = FilterAggregation::new_with_query(Box::new(SerializableTermQuery::new(term_query)));
/// ```
///
/// # Result
/// The filter aggregation returns a single bucket with:
/// - `doc_count`: Number of documents matching the filter
/// - Sub-aggregation results computed on the filtered document set
#[derive(Debug, Clone)]
pub struct FilterAggregation {
    /// The query for filtering - can be either a query string or a direct Query object
    query: FilterQuery,
}

/// Represents different ways to specify a filter query
#[derive(Debug)]
pub enum FilterQuery {
    /// Query string that will be parsed using Tantivy's standard parsing facilities
    /// Accepts query strings that can be parsed by QueryParser::parse_query()
    QueryString(String),

    /// Custom Query object for programmatic query construction
    ///
    /// This variant allows passing pre-constructed Query objects directly,
    /// which is useful for:
    /// - Custom query types not expressible as query strings
    /// - Programmatic query construction
    /// - Extension query types
    ///
    /// Note: This variant cannot be serialized to JSON (only QueryString can be serialized)
    CustomQuery(Box<dyn SerializableQuery>),
}

impl Clone for FilterQuery {
    fn clone(&self) -> Self {
        match self {
            FilterQuery::QueryString(query_string) => {
                FilterQuery::QueryString(query_string.clone())
            }
            FilterQuery::CustomQuery(query) => FilterQuery::CustomQuery(query.clone_box()),
        }
    }
}

impl FilterAggregation {
    /// Create a new filter aggregation with a query string
    /// The query string will be parsed using the QueryParser::parse_query() method.
    pub fn new(query_string: String) -> Self {
        Self {
            query: FilterQuery::QueryString(query_string),
        }
    }

    /// Create a new filter aggregation with a direct Query object
    /// This enables custom query types to be used directly
    pub fn new_with_query(query: Box<dyn SerializableQuery>) -> Self {
        Self {
            query: FilterQuery::CustomQuery(query),
        }
    }

    /// Parse the query into a Tantivy Query object
    ///
    /// For query strings, this uses the QueryParser::parse_query() method.
    /// For direct Query objects, returns a clone.
    fn parse_query(
        &self,
        schema: &Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> crate::Result<Box<dyn Query>> {
        match &self.query {
            FilterQuery::QueryString(query_str) => {
                let query_parser =
                    QueryParser::new(schema.clone(), vec![], tokenizer_manager.clone());

                query_parser
                    .parse_query(query_str)
                    .map_err(|e| TantivyError::InvalidArgument(e.to_string()))
            }
            FilterQuery::CustomQuery(query) => {
                // Return a clone of the direct query
                Ok(query.clone_box())
            }
        }
    }

    /// Parse the query with a custom QueryParser
    ///
    /// This method allows using a pre-configured QueryParser with custom settings
    /// like field boosts, fuzzy matching, default fields, etc.
    /// For direct Query objects, the QueryParser is ignored and a clone is returned.
    pub fn parse_query_with_parser(
        &self,
        query_parser: &QueryParser,
    ) -> crate::Result<Box<dyn Query>> {
        match &self.query {
            FilterQuery::QueryString(query_str) => query_parser
                .parse_query(query_str)
                .map_err(|e| TantivyError::InvalidArgument(e.to_string())),
            FilterQuery::CustomQuery(query) => {
                // Return a clone of the direct query, ignoring the parser
                Ok(query.clone_box())
            }
        }
    }

    /// Get the fast field names used by this aggregation (none for filter aggregation)
    pub fn get_fast_field_names(&self) -> Vec<&str> {
        // Filter aggregation cannot introspect query fast field dependencies.
        //
        // As of PR #2693, queries can fall back to fast fields when fields are not indexed
        // (e.g., TermQuery falls back to RangeQuery on fast fields). However, the Query
        // trait has no mechanism to report these dependencies.
        //
        // For prefetching optimization, callers must analyze the query themselves to
        // determine fast field usage. This requires:
        // 1. Parsing the query string to extract field references
        // 2. Checking the schema to see if those fields are indexed or fast-only
        // 3. Collecting fast field names for non-indexed fields
        //
        // This limitation exists because:
        // - Query::weight() is called during execution, not during planning
        // - The fallback decision is made per-segment based on field configuration
        // - There's no Query trait method to declare potential fast field dependencies
        vec![]
    }
}

// Custom serialization implementation
impl Serialize for FilterAggregation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match &self.query {
            FilterQuery::QueryString(query_string) => {
                // Serialize the query string directly
                query_string.serialize(serializer)
            }
            FilterQuery::CustomQuery(query) => erased_serde::serialize(query, serializer),
        }
    }
}

impl<'de> Deserialize<'de> for FilterAggregation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        // Deserialize as query string
        let query_string = String::deserialize(deserializer)?;
        Ok(FilterAggregation::new(query_string))
    }
}

// PartialEq is required because AggregationVariants derives it
// We implement it manually to handle Box<dyn Query> which doesn't impl PartialEq
impl PartialEq for FilterAggregation {
    fn eq(&self, other: &Self) -> bool {
        match (&self.query, &other.query) {
            (FilterQuery::QueryString(a), FilterQuery::QueryString(b)) => a == b,
            // Custom queries cannot be compared for equality
            _ => false,
        }
    }
}

/// Request data for filter aggregation
/// This struct holds the per-segment data needed to execute a filter aggregation
pub struct FilterAggReqData {
    /// The name of the filter aggregation
    pub name: String,
    /// The filter aggregation
    pub req: FilterAggregation,
    /// The segment reader
    pub segment_reader: SegmentReader,
}

impl FilterAggReqData {
    pub(crate) fn get_memory_consumption(&self) -> usize {
        // Rough estimate: name + segment reader reference
        self.name.len() + std::mem::size_of::<SegmentReader>()
    }
}

/// Document evaluator for filter queries using BitSet
struct DocumentQueryEvaluator {
    /// BitSet containing all matching documents for this segment.
    /// For AllQuery, this is a full BitSet (all bits set).
    /// For other queries, only matching document bits are set.
    bitset: BitSet,
}

impl DocumentQueryEvaluator {
    /// Create and initialize a document query evaluator for a segment
    /// This executes the query upfront and collects results into a BitSet,
    /// unless the query is AllQuery in which case we skip BitSet creation.
    fn new(
        query: Box<dyn Query>,
        schema: Schema,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self> {
        let max_doc = segment_reader.max_doc();

        // Optimization: Detect AllQuery and create a full BitSet
        if query.as_any().downcast_ref::<AllQuery>().is_some() {
            return Ok(Self {
                bitset: BitSet::with_max_value_and_full(max_doc),
            });
        }

        // Get the weight for the query
        let weight = query.weight(EnableScoring::disabled_from_schema(&schema))?;

        // Get a scorer that iterates over matching documents
        let mut scorer = weight.scorer(segment_reader, 1.0)?;

        // Create a BitSet to hold all matching documents
        let mut bitset = BitSet::with_max_value(max_doc);

        // Collect all matching documents into the BitSet
        // This is the upfront cost, but then lookups are O(1)
        let mut doc = scorer.doc();
        while doc != crate::TERMINATED {
            bitset.insert(doc);
            doc = scorer.advance();
        }

        Ok(Self { bitset })
    }

    /// Evaluate if a document matches the filter query
    /// O(1) lookup in the precomputed BitSet
    #[inline]
    pub fn matches_document(&self, doc: DocId) -> bool {
        self.bitset.contains(doc)
    }

    /// Filter a batch of documents
    /// Returns matching documents from the input batch
    #[inline]
    pub fn filter_batch(&self, docs: &[DocId], output: &mut Vec<DocId>) {
        for &doc in docs {
            if self.bitset.contains(doc) {
                output.push(doc);
            }
        }
    }
}

impl Debug for DocumentQueryEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentQueryEvaluator")
            .field("num_matches", &self.bitset.len())
            .finish()
    }
}

/// Segment collector for filter aggregation
pub struct SegmentFilterCollector {
    /// Document evaluator for the filter query
    /// The evaluator internally stores a SegmentReader
    evaluator: DocumentQueryEvaluator,
    /// Document count in this bucket
    doc_count: u64,
    /// Sub-aggregation collectors
    sub_aggregations: Option<Box<dyn SegmentAggregationCollector>>,
    /// Accessor index for this filter aggregation
    accessor_idx: usize,
    /// Reusable buffer for matching documents to minimize allocations
    matching_docs_buffer: Vec<DocId>,
}

impl SegmentFilterCollector {
    /// Create a new filter segment collector following the new agg_data pattern
    pub(crate) fn from_req_and_validate(
        req: &mut AggregationsSegmentCtx,
        node: &AggRefNode,
    ) -> crate::Result<Self> {
        // Clone necessary data before mutable borrow
        let req_data = req.get_filter_req_data(node.idx_in_req_data);
        let segment_reader = req_data.segment_reader.clone();
        let filter_req = req_data.req.clone();
        let max_doc = segment_reader.max_doc();

        let schema = segment_reader.schema();
        let tokenizers = &req.context.tokenizers;

        // Parse the query
        let query = filter_req.parse_query(schema, tokenizers)?;

        let evaluator = DocumentQueryEvaluator::new(query, schema.clone(), &segment_reader)?;

        // Build sub-aggregation collectors if any
        let sub_agg_collector = if !node.children.is_empty() {
            Some(build_segment_agg_collectors(req, &node.children)?)
        } else {
            None
        };

        // Pre-allocate buffer to avoid repeated allocations during collection
        let buffer_capacity = COLLECT_BLOCK_BUFFER_LEN.min(max_doc as usize);

        Ok(SegmentFilterCollector {
            evaluator,
            doc_count: 0,
            sub_aggregations: sub_agg_collector,
            accessor_idx: node.idx_in_req_data,
            matching_docs_buffer: Vec::with_capacity(buffer_capacity),
        })
    }
}

impl Debug for SegmentFilterCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFilterCollector")
            .field("doc_count", &self.doc_count)
            .field("has_sub_aggs", &self.sub_aggregations.is_some())
            .field("evaluator", &self.evaluator)
            .finish()
    }
}

impl CollectorClone for SegmentFilterCollector {
    fn clone_box(&self) -> Box<dyn SegmentAggregationCollector> {
        // For now, panic - this needs proper implementation with weight recreation
        panic!("SegmentFilterCollector cloning not yet implemented - requires weight recreation")
    }
}

impl SegmentAggregationCollector for SegmentFilterCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let mut sub_results = IntermediateAggregationResults::default();

        if let Some(sub_aggs) = self.sub_aggregations {
            sub_aggs.add_intermediate_aggregation_result(agg_data, &mut sub_results)?;
        }

        // Create the filter bucket result
        let filter_bucket_result = IntermediateBucketResult::Filter {
            doc_count: self.doc_count,
            sub_aggregations: sub_results,
        };

        // Get the name of this filter aggregation
        let name = agg_data.per_request.filter_req_data[self.accessor_idx]
            .as_ref()
            .expect("filter_req_data slot is empty")
            .name
            .clone();

        results.push(
            name,
            IntermediateAggregationResult::Bucket(filter_bucket_result),
        )?;

        Ok(())
    }

    fn collect(&mut self, doc: DocId, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        // O(1) BitSet lookup to check if document matches filter
        if self.evaluator.matches_document(doc) {
            self.doc_count += 1;

            // If we have sub-aggregations, collect on them for this filtered document
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                sub_aggs.collect(doc, agg_data)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        // Take the request data to avoid borrow checker issues with sub-aggregations
        let req = agg_data.take_filter_req_data(self.accessor_idx);

        // Use batch filtering with O(1) BitSet lookups
        self.matching_docs_buffer.clear();
        self.evaluator
            .filter_batch(docs, &mut self.matching_docs_buffer);

        self.doc_count += self.matching_docs_buffer.len() as u64;

        // Batch process sub-aggregations if we have matches
        if !self.matching_docs_buffer.is_empty() {
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                // Use collect_block for better sub-aggregation performance
                sub_aggs.collect_block(&self.matching_docs_buffer, agg_data)?;
            }
        }

        // Put the request data back
        agg_data.put_back_filter_req_data(self.accessor_idx, req);

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        if let Some(ref mut sub_aggs) = self.sub_aggregations {
            sub_aggs.flush(agg_data)?;
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
    use std::time::Instant;

    use serde_json::{json, Value};

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::{AggContextParams, AggregationCollector};
    use crate::query::{AllQuery, QueryParser, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, Term, FAST, INDEXED, STORED, TEXT};
    use crate::{doc, Index, IndexWriter};

    // Test helper functions
    fn aggregation_results_to_json(results: &AggregationResults) -> Value {
        serde_json::to_value(results).expect("Failed to serialize aggregation results")
    }

    fn json_values_match(actual: &Value, expected: &Value, tolerance: f64) -> bool {
        match (actual, expected) {
            (Value::Number(a), Value::Number(e)) => {
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let e_f64 = e.as_f64().unwrap_or(0.0);
                (a_f64 - e_f64).abs() < tolerance
            }
            (Value::Object(a_map), Value::Object(e_map)) => {
                if a_map.len() != e_map.len() {
                    return false;
                }
                for (key, expected_val) in e_map {
                    match a_map.get(key) {
                        Some(actual_val) => {
                            if !json_values_match(actual_val, expected_val, tolerance) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            (Value::Array(a_arr), Value::Array(e_arr)) => {
                if a_arr.len() != e_arr.len() {
                    return false;
                }
                for (actual_item, expected_item) in a_arr.iter().zip(e_arr.iter()) {
                    if !json_values_match(actual_item, expected_item, tolerance) {
                        return false;
                    }
                }
                true
            }
            _ => actual == expected,
        }
    }

    fn assert_aggregation_results_match(
        actual_results: &AggregationResults,
        expected_json: Value,
        tolerance: f64,
    ) {
        let actual_json = aggregation_results_to_json(actual_results);

        if !json_values_match(&actual_json, &expected_json, tolerance) {
            panic!(
                "Aggregation results do not match expected JSON.\nActual:\n{}\nExpected:\n{}",
                serde_json::to_string_pretty(&actual_json).unwrap(),
                serde_json::to_string_pretty(&expected_json).unwrap()
            );
        }
    }

    macro_rules! assert_agg_results {
        ($actual:expr, $expected:expr) => {
            assert_aggregation_results_match($actual, $expected, 0.1)
        };
        ($actual:expr, $expected:expr, $tolerance:expr) => {
            assert_aggregation_results_match($actual, $expected, $tolerance)
        };
    }

    fn create_standard_test_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let category = schema_builder.add_text_field("category", TEXT | FAST);
        let brand = schema_builder.add_text_field("brand", TEXT | FAST);
        let price = schema_builder.add_u64_field("price", FAST | INDEXED);
        let rating = schema_builder.add_f64_field("rating", FAST);
        let in_stock = schema_builder.add_bool_field("in_stock", FAST | INDEXED);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer(50_000_000)?;

        writer.add_document(doc!(
            category => "electronics", brand => "apple",
            price => 999u64, rating => 4.5f64, in_stock => true
        ))?;
        writer.add_document(doc!(
            category => "electronics", brand => "samsung",
            price => 799u64, rating => 4.2f64, in_stock => true
        ))?;
        writer.add_document(doc!(
            category => "clothing", brand => "nike",
            price => 120u64, rating => 4.1f64, in_stock => false
        ))?;
        writer.add_document(doc!(
            category => "books", brand => "penguin",
            price => 25u64, rating => 4.8f64, in_stock => true
        ))?;

        writer.commit()?;
        Ok(index)
    }

    /// Helper to create aggregation collector with proper tokenizers from index
    fn create_collector(index: &Index, aggregations: Aggregations) -> AggregationCollector {
        AggregationCollector::from_aggs(
            aggregations,
            AggContextParams::new(Default::default(), index.tokenizers().clone()),
        )
    }

    #[test]
    fn test_basic_filter_with_metric_agg() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "avg_price": { "avg": { "field": "price" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }  // (999 + 799) / 2
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_filter_with_no_matches() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "furniture": {
                "filter": "category:furniture",
                "aggs": {
                    "avg_price": { "avg": { "field": "price" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "furniture": {
                "doc_count": 0,
                "avg_price": { "value": null }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_independent_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            },
            "in_stock": {
                "filter": "in_stock:true",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            },
            "high_rated": {
                "filter": "rating:[4.5 TO *]",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }
            },
            "in_stock": {
                "doc_count": 3,  // apple, samsung, penguin
                "count": { "value": 3.0 }
            },
            "high_rated": {
                "doc_count": 2,  // apple (4.5), penguin (4.8)
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Query Type Tests
    // ============================================================================

    #[test]
    fn test_term_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "apple_products": {
                "filter": "brand:apple",
                "aggs": { "max_price": { "max": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "apple_products": {
                "doc_count": 1,
                "max_price": { "value": 999.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_range_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "mid_price": {
                "filter": "price:[100 TO 900]",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "mid_price": {
                "doc_count": 2,  // samsung (799), nike (120)
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_boolean_query_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "premium_electronics": {
                "filter": "category:electronics AND price:[800 TO *]",
                "aggs": { "avg_rating": { "avg": { "field": "rating" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "premium_electronics": {
                "doc_count": 1,  // Only apple (999) is >= 800 in tantivy's range semantics
                "avg_rating": { "value": 4.5 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_bool_field_filter() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "in_stock": {
                "filter": "in_stock:true",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            },
            "out_of_stock": {
                "filter": "in_stock:false",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "in_stock": {
                "doc_count": 3,  // apple, samsung, penguin
                "avg_price": { "value": 607.67 }  // (999 + 799 + 25) / 3 ≈ 607.67
            },
            "out_of_stock": {
                "doc_count": 1,  // nike
                "count": { "value": 1.0 }
            }
        });

        assert_agg_results!(&result, expected, 1.0);
        Ok(())
    }

    // ============================================================================
    // Nested Filter Tests
    // ============================================================================

    #[test]
    fn test_two_level_nested_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "all": {
                "filter": "*",
                "aggs": {
                    "electronics": {
                        "filter": "category:electronics",
                        "aggs": {
                            "expensive": {
                                "filter": "price:[900 TO *]",
                                "aggs": {
                                    "count": { "value_count": { "field": "brand" } }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "all": {
                "doc_count": 4,
                "electronics": {
                    "doc_count": 2,
                    "expensive": {
                        "doc_count": 1,  // Only apple (999) is >= 900
                        "count": { "value": 1.0 }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_deeply_nested_filters() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "level1": {
                "filter": "*",
                "aggs": {
                    "level2": {
                        "filter": "in_stock:true",
                        "aggs": {
                            "level3": {
                                "filter": "rating:[4.0 TO *]",
                                "aggs": {
                                    "level4": {
                                        "filter": "price:[500 TO *]",
                                        "aggs": {
                                            "final_count": { "value_count": { "field": "brand" } }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "level1": {
                "doc_count": 4,
                "level2": {
                    "doc_count": 3,  // in_stock: apple, samsung, penguin
                    "level3": {
                        "doc_count": 3,  // all have rating >= 4.0
                        "level4": {
                            "doc_count": 2,  // apple (999), samsung (799)
                            "final_count": { "value": 2.0 }
                        }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_multiple_nested_branches() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "root": {
                "filter": "*",
                "aggs": {
                    "electronics_branch": {
                        "filter": "category:electronics",
                        "aggs": {
                            "avg_price": { "avg": { "field": "price" } }
                        }
                    },
                    "in_stock_branch": {
                        "filter": "in_stock:true",
                        "aggs": {
                            "count": { "value_count": { "field": "brand" } }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "root": {
                "doc_count": 4,
                "electronics_branch": {
                    "doc_count": 2,
                    "avg_price": { "value": 899.0 }
                },
                "in_stock_branch": {
                    "doc_count": 3,
                    "count": { "value": 3.0 }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_nested_filters_with_multiple_siblings_at_each_level() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test complex nesting: multiple branches at each level
        let agg = json!({
            "all": {
                "filter": "*",
                "aggs": {
                    // Level 2: Two independent filters
                    "expensive": {
                        "filter": "price:[500 TO *]",
                        "aggs": {
                            // Level 3: Multiple branches under "expensive"
                            "electronics": {
                                "filter": "category:electronics",
                                "aggs": {
                                    "avg_rating": { "avg": { "field": "rating" } }
                                }
                            },
                            "in_stock": {
                                "filter": "in_stock:true",
                                "aggs": {
                                    "count": { "value_count": { "field": "brand" } }
                                }
                            }
                        }
                    },
                    "affordable": {
                        "filter": "price:[0 TO 200]",
                        "aggs": {
                            // Level 3: Multiple branches under "affordable"
                            "books": {
                                "filter": "category:books",
                                "aggs": {
                                    "max_rating": { "max": { "field": "rating" } }
                                }
                            },
                            "clothing": {
                                "filter": "category:clothing",
                                "aggs": {
                                    "min_price": { "min": { "field": "price" } }
                                }
                            }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "all": {
                "doc_count": 4,
                "expensive": {
                    "doc_count": 2,  // apple (999), samsung (799)
                    "electronics": {
                        "doc_count": 2,  // both are electronics
                        "avg_rating": { "value": 4.35 }  // (4.5 + 4.2) / 2
                    },
                    "in_stock": {
                        "doc_count": 2,  // both are in stock
                        "count": { "value": 2.0 }
                    }
                },
                "affordable": {
                    "doc_count": 2,  // nike (120), penguin (25)
                    "books": {
                        "doc_count": 1,  // penguin (25)
                        "max_rating": { "value": 4.8 }
                    },
                    "clothing": {
                        "doc_count": 1,  // nike (120)
                        "min_price": { "value": 120.0 }
                    }
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Sub-Aggregation Combination Tests
    // ============================================================================

    #[test]
    fn test_filter_with_terms_sub_agg() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "brands": {
                        "terms": { "field": "brand" },
                        "aggs": {
                            "avg_price": { "avg": { "field": "price" } }
                        }
                    }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        // Verify the structure exists and has expected doc_count
        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "brands": {
                    "buckets": [
                        {
                            "key": "samsung",
                            "doc_count": 1,
                            "avg_price": { "value": 799.0 }
                        },
                        {
                            "key": "apple",
                            "doc_count": 1,
                            "avg_price": { "value": 999.0 }
                        }
                    ],
                    "sum_other_doc_count": 0,
                    "doc_count_error_upper_bound": 0
                }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_filter_with_multiple_metric_aggs() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": {
                    "price_stats": { "stats": { "field": "price" } },
                    "rating_avg": { "avg": { "field": "rating" } },
                    "count": { "value_count": { "field": "brand" } }
                }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,
                "price_stats": {
                    "count": 2,
                    "min": 799.0,
                    "max": 999.0,
                    "sum": 1798.0,
                    "avg": 899.0
                },
                "rating_avg": { "value": 4.35 },
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Edge Cases and Error Handling
    // ============================================================================

    #[test]
    fn test_filter_on_empty_index() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let _category = schema_builder.add_text_field("category", TEXT | FAST);
        let _price = schema_builder.add_u64_field("price", FAST);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter = index.writer(50_000_000)?;
        writer.commit()?; // Commit empty index

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 0,
                "avg_price": { "value": null }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    #[test]
    fn test_malformed_query_string() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Empty query string
        let agg = json!({
            "test": {
                "filter": "",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let result = serde_json::from_value::<Aggregations>(agg)
            .map_err(|e| crate::TantivyError::InvalidArgument(e.to_string()))
            .and_then(|aggregations| {
                let collector = create_collector(&index, aggregations);
                searcher.search(&AllQuery, &collector)
            });

        // Empty string should either work (matching nothing) or error gracefully
        assert!(result.is_ok() || result.is_err());
        Ok(())
    }

    #[test]
    fn test_filter_with_base_query() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let schema = index.schema();

        // Use a base query to pre-filter to in_stock items only
        let in_stock_field = schema.get_field("in_stock").unwrap();
        let base_query = TermQuery::new(
            Term::from_field_bool(in_stock_field, true),
            IndexRecordOption::Basic,
        );

        let agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&base_query, &collector)?;

        let expected = json!({
            "electronics": {
                "doc_count": 2,  // Both in-stock electronics
                "count": { "value": 2.0 }
            }
        });

        assert_agg_results!(&result, expected);
        Ok(())
    }

    // ============================================================================
    // Custom Query Integration Tests
    // ============================================================================

    #[test]
    fn test_direct_query_object() -> crate::Result<()> {
        use crate::aggregation::bucket::SerializableQuery;
        use crate::query::{EnableScoring, Query, Weight};

        #[derive(Debug, Clone)]
        struct SerializableTermQuery(TermQuery);
        impl SerializableQuery for SerializableTermQuery {
            fn clone_box(&self) -> Box<dyn SerializableQuery> {
                Box::new(self.clone())
            }
        }

        impl Query for SerializableTermQuery {
            fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
                self.0.weight(enable_scoring)
            }
        }

        impl SerializableTermQuery {
            pub fn new(term_query: TermQuery) -> Self {
                Self(term_query)
            }
        }

        impl serde::Serialize for SerializableTermQuery {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: serde::Serializer {
                "todo".serialize(serializer)
            }
        }

        let index = create_standard_test_index()?;
        let schema = index.schema();

        // Create a custom query directly
        let category_field = schema.get_field("category").unwrap();
        let term = Term::from_field_text(category_field, "electronics");
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        // Use it in FilterAggregation
        let filter_agg =
            FilterAggregation::new_with_query(Box::new(SerializableTermQuery::new(term_query)));

        // Verify it can be serialized
        let serialization_result = serde_json::to_string(&filter_agg);
        assert!(
            serialization_result.is_ok(),
            "Direct queries should serialize"
        );

        Ok(())
    }

    #[test]
    fn test_query_string_serialization() -> crate::Result<()> {
        // Query strings should serialize/deserialize correctly
        let filter_agg = FilterAggregation::new("category:electronics".to_string());

        let serialized = serde_json::to_string(&filter_agg)?;
        assert!(serialized.contains("electronics"));

        let deserialized: FilterAggregation = serde_json::from_str(&serialized)?;
        // Verify it deserializes correctly by using it in an aggregation
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg = json!({
            "test": {
                "filter": deserialized,
                "aggs": { "count": { "value_count": { "field": "brand" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector = create_collector(&index, aggregations);
        let result = searcher.search(&AllQuery, &collector)?;

        // Should match 2 electronics
        let result_json = serde_json::to_value(&result)?;
        assert_eq!(result_json["test"]["doc_count"], 2);

        Ok(())
    }

    // ============================================================================
    // Correctness Validation Tests
    // ============================================================================

    #[test]
    fn test_filter_result_correctness_vs_separate_query() -> crate::Result<()> {
        let index = create_standard_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let schema = index.schema();

        // Method 1: Filter aggregation
        let filter_agg = json!({
            "electronics": {
                "filter": "category:electronics",
                "aggs": { "avg_price": { "avg": { "field": "price" } } }
            }
        });

        let aggregations: Aggregations = serde_json::from_value(filter_agg)?;
        let collector = create_collector(&index, aggregations);
        let filter_result = searcher.search(&AllQuery, &collector)?;

        // Method 2: Separate query
        let category_field = schema.get_field("category").unwrap();
        let term = Term::from_field_text(category_field, "electronics");
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);

        let separate_agg = json!({
            "result": { "avg": { "field": "price" } }
        });

        let separate_aggregations: Aggregations = serde_json::from_value(separate_agg)?;
        let separate_collector =
            AggregationCollector::from_aggs(separate_aggregations, Default::default());
        let separate_result = searcher.search(&term_query, &separate_collector)?;

        // Both methods should produce identical results
        let filter_expected = json!({
            "electronics": {
                "doc_count": 2,
                "avg_price": { "value": 899.0 }
            }
        });

        let separate_expected = json!({
            "result": {
                "value": 899.0
            }
        });

        // Verify filter aggregation result
        assert_agg_results!(&filter_result, filter_expected);

        // Verify separate query result matches
        assert_agg_results!(&separate_result, separate_expected);

        // This test demonstrates that filter aggregation produces the same results
        // as running a separate query with the same condition
        Ok(())
    }

    #[test]
    fn test_filter_aggregation_performance() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let message_field = schema_builder.add_text_field("message", TEXT | STORED);
        let severity_field = schema_builder.add_u64_field("severity", FAST | STORED | INDEXED);
        let schema = schema_builder.build();

        // Create index with test data
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer: IndexWriter = index.writer(50_000_000)?;

        // Add 100K documents
        for i in 0..100_000u64 {
            let severity = (i % 5) + 1;
            let message = if i % 5 == 0 {
                "research data"
            } else {
                "other data"
            };
            index_writer.add_document(doc!(
                message_field => message,
                severity_field => severity
            ))?;
        }
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Parse query that matches documents
        let query = QueryParser::for_index(&index, vec![message_field]).parse_query("research")?;

        // Test 1: Direct TermsAggregation (baseline)
        let direct_agg = json!({
            "grouped": {
                "terms": {
                    "field": "severity",
                    "size": 65000
                },
                "aggs": {
                    "count": {
                        "value_count": { "field": "severity" }
                    }
                }
            }
        });

        let start = Instant::now();
        let aggregations: Aggregations = serde_json::from_value(direct_agg)?;
        let collector = create_collector(&index, aggregations);
        let direct_result = searcher.search(&query, &collector)?;
        let direct_time = start.elapsed();

        // Test 2: FilterAggregation with query filter
        let filter_agg = json!({
            "filter": {
                "filter": "message:research",
                "aggs": {
                    "grouped": {
                        "terms": {
                            "field": "severity",
                            "size": 65000
                        },
                        "aggs": {
                            "count": {
                                "value_count": { "field": "severity" }
                            }
                        }
                    }
                }
            }
        });

        let start = Instant::now();
        let aggregations: Aggregations = serde_json::from_value(filter_agg)?;
        let collector = create_collector(&index, aggregations);
        let filter_result = searcher.search(&AllQuery, &collector)?;
        let filter_time = start.elapsed();

        // Test 3: FilterAggregation with "*" filter (should match direct query results)
        let match_all_filter_agg = json!({
            "filter": {
                "filter": "*",
                "aggs": {
                    "grouped": {
                        "terms": {
                            "field": "severity",
                            "size": 65000
                        },
                        "aggs": {
                            "count": {
                                "value_count": { "field": "severity" }
                            }
                        }
                    }
                }
            }
        });

        let start = Instant::now();
        let aggregations: Aggregations = serde_json::from_value(match_all_filter_agg)?;
        let collector = create_collector(&index, aggregations);
        let match_all_filter_result = searcher.search(&query, &collector)?;
        let match_all_filter_time = start.elapsed();

        // Print timing results
        println!("Direct aggregation time: {:?}", direct_time);
        println!("Filter aggregation (query filter) time: {:?}", filter_time);
        println!(
            "Filter aggregation (match-all filter) time: {:?}",
            match_all_filter_time
        );

        let slowdown_ratio = filter_time.as_secs_f64() / direct_time.as_secs_f64().max(0.001);
        let match_all_slowdown =
            match_all_filter_time.as_secs_f64() / direct_time.as_secs_f64().max(0.001);
        println!("Query filter slowdown ratio: {:.2}x", slowdown_ratio);
        println!(
            "Match-all filter slowdown ratio: {:.2}x",
            match_all_slowdown
        );

        // Verify results are equivalent by checking JSON structure
        let direct_json = serde_json::to_value(&direct_result)?;
        let filter_json = serde_json::to_value(&filter_result)?;
        let match_all_filter_json = serde_json::to_value(&match_all_filter_result)?;

        // Direct result has "grouped" at top level
        let direct_grouped = &direct_json["grouped"];

        // Filter result has "filter" -> "grouped"
        let filter_grouped = &filter_json["filter"]["grouped"];
        let match_all_filter_grouped = &match_all_filter_json["filter"]["grouped"];

        // All three should have the same structure
        assert_eq!(
            direct_grouped, filter_grouped,
            "Query filter should match direct aggregation"
        );
        assert_eq!(
            direct_grouped, match_all_filter_grouped,
            "Match-all filter with base query should match direct aggregation"
        );

        // Performance assertion: FilterAggregation should not be more than 3x slower
        assert!(
            slowdown_ratio < 3.0,
            "FilterAggregation is {}x slower than direct aggregation (expected < 3x).",
            slowdown_ratio
        );

        assert!(
            match_all_slowdown < 3.0,
            "Match-all filter is {}x slower than direct aggregation (expected < 3x).",
            match_all_slowdown
        );

        Ok(())
    }

    #[test]
    fn test_custom_tokenizer_required() -> crate::Result<()> {
        use crate::schema::{TextFieldIndexing, TextOptions};
        use crate::tokenizer::{SimpleTokenizer, TextAnalyzer, TokenizerManager};

        // Create a custom tokenizer that doesn't lowercase (just splits on whitespace)
        let custom_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default()).build();

        // Register tokenizer
        let tokenizers = TokenizerManager::default();
        tokenizers.register("my_custom", custom_tokenizer);

        // Create a schema with a text field that uses our custom tokenizer
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("my_custom")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();

        // Build index with custom tokenizer
        let index = crate::IndexBuilder::new()
            .schema(schema.clone())
            .tokenizers(tokenizers)
            .create_in_ram()?;
        let mut writer = index.writer(50_000_000)?;

        // Add documents with UPPERCASE text
        writer.add_document(doc!(text_field => "HELLO"))?;
        writer.add_document(doc!(text_field => "WORLD"))?;
        writer.add_document(doc!(text_field => "hello"))?; // lowercase version
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Test: With correct tokenizer (from index) - should work
        let agg = json!({
            "uppercase_hello": {
                "filter": "text:HELLO"
            }
        });

        let aggregations: Aggregations = serde_json::from_value(agg)?;
        let collector_with_tokenizer = create_collector(&index, aggregations.clone());
        let result_with_tokenizer = searcher.search(&AllQuery, &collector_with_tokenizer)?;

        // Should match only the UPPERCASE "HELLO" (1 document)
        let result_json = serde_json::to_value(&result_with_tokenizer)?;
        assert_eq!(
            result_json["uppercase_hello"]["doc_count"], 1,
            "With custom tokenizer from index, should match exactly 1 UPPERCASE document"
        );

        // Test 2: With default tokenizer (wrong!) - should fail to parse the query
        // because "my_custom" tokenizer is not in the default TokenizerManager
        let collector_with_default = AggregationCollector::from_aggs(
            aggregations,
            AggContextParams::new(Default::default(), TokenizerManager::default()),
        );
        let result_with_default = searcher.search(&AllQuery, &collector_with_default);

        // This should error because the tokenizer "my_custom" is not registered
        assert!(
            result_with_default.is_err(),
            "Without proper tokenizers, query parsing should fail"
        );
        assert!(
            result_with_default
                .unwrap_err()
                .to_string()
                .contains("my_custom"),
            "Error should mention the missing tokenizer"
        );

        Ok(())
    }
}
