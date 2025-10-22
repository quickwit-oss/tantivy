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

/// Filter aggregation creates a single bucket containing documents that match a query.
///
/// # Usage
/// ```rust
/// use tantivy::aggregation::bucket::filter::FilterAggregation;
/// use tantivy::query::TermQuery;
///
/// // Query strings are parsed using Tantivy's standard QueryParser
/// let filter_agg = FilterAggregation::new("category:electronics AND price:[100 TO 500]".to_string());
///
/// // Direct Query objects can be used for custom query types
/// let term_query = TermQuery::new(
///     tantivy::Term::from_field_text(tantivy::schema::Field::from_field_id(0), "electronics"),
///     tantivy::schema::IndexRecordOption::Basic
/// );
/// let filter_agg = FilterAggregation::new_with_query(Box::new(term_query));
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
    CustomQuery(Box<dyn Query>),
}

impl Clone for FilterQuery {
    fn clone(&self) -> Self {
        match self {
            FilterQuery::QueryString(query_string) => {
                FilterQuery::QueryString(query_string.clone())
            }
            FilterQuery::CustomQuery(query) => FilterQuery::CustomQuery(query.box_clone()),
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
    pub fn new_with_query(query: Box<dyn Query>) -> Self {
        Self {
            query: FilterQuery::CustomQuery(query),
        }
    }

    /// Parse the query into a Tantivy Query object
    ///
    /// For query strings, this uses the QueryParser::parse_query() method.
    /// For direct Query objects, returns a clone.
    fn parse_query(&self, schema: &Schema) -> crate::Result<Box<dyn Query>> {
        match &self.query {
            FilterQuery::QueryString(query_str) => {
                let tokenizer_manager = TokenizerManager::default();
                let query_parser = QueryParser::new(schema.clone(), vec![], tokenizer_manager);

                query_parser
                    .parse_query(query_str)
                    .map_err(|e| TantivyError::InvalidArgument(e.to_string()))
            }
            FilterQuery::CustomQuery(query) => {
                // Return a clone of the direct query
                Ok(query.box_clone())
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
                Ok(query.box_clone())
            }
        }
    }

    /// Get the fast field names used by this aggregation (none for filter aggregation)
    pub fn get_fast_field_names(&self) -> Vec<&str> {
        // Filter aggregation doesn't declare fast field dependencies directly
        // Note: The query may internally use fast fields for non-indexed fields,
        // but this is handled by the query execution layer, not aggregation
        vec![]
    }
}

// Custom serialization implementation
impl Serialize for FilterAggregation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.query {
            FilterQuery::QueryString(query_string) => {
                // Serialize the query string directly
                query_string.serialize(serializer)
            }
            FilterQuery::CustomQuery(_) => {
                // Custom queries cannot be serialized
                Err(serde::ser::Error::custom(
                    "Custom Query objects cannot be serialized. Use query strings for \
                     serialization support.",
                ))
            }
        }
    }
}

impl<'de> Deserialize<'de> for FilterAggregation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
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

        // Parse the query
        let query = filter_req.parse_query(schema)?;

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
