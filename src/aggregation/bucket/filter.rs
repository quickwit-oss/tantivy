use std::fmt::Debug;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector_with_reader, CollectorClone, SegmentAggregationCollector,
};
use crate::docset::{DocSet, COLLECT_BLOCK_BUFFER_LEN};
use crate::query::{Query, QueryParser, Scorer};
use crate::schema::Schema;
use crate::tokenizer::TokenizerManager;
use crate::{DocId, SegmentReader, TantivyError, TERMINATED};

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

/// Document evaluator for filter queries
/// This avoids running separate query executions and instead evaluates queries per document
struct DocumentQueryEvaluator {
    /// The scorer for document matching
    /// We create this once per segment and reuse it for all document checks.
    /// This is critical for performance.
    scorer: Box<dyn Scorer>,
}

impl DocumentQueryEvaluator {
    /// Create and initialize a document query evaluator for a segment
    fn new(
        query: Box<dyn Query>,
        schema: Schema,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self> {
        use crate::query::EnableScoring;
        let weight = query.weight(EnableScoring::disabled_from_schema(&schema))?;
        let scorer = weight.scorer(segment_reader, 1.0)?;
        Ok(Self { scorer })
    }

    /// Evaluate if a document matches the filter query
    /// This is the core performance-critical method
    pub fn matches_document(&mut self, doc: DocId) -> crate::Result<bool> {
        let scorer = &mut self.scorer;

        // Use the same pattern as Weight::explain to handle seek ordering correctly
        // The scorer maintains its position, so we can efficiently check if doc matches
        Ok(!(scorer.doc() > doc || scorer.seek(doc) != doc))
    }

    /// Filter a batch of documents efficiently using intersection
    /// Returns matching documents from the input batch
    #[inline]
    pub fn filter_batch(&mut self, docs: &[DocId], output: &mut Vec<DocId>) -> crate::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        let scorer = &mut self.scorer;

        // Efficient intersection: advance scorer and check against input docs
        let mut scorer_doc = scorer.doc();
        if scorer_doc == TERMINATED {
            return Ok(());
        }

        for &doc in docs {
            // Advance scorer to at least doc
            if scorer_doc < doc {
                scorer_doc = scorer.seek(doc);
            }

            // If scorer matches this doc, include it
            if scorer_doc == doc {
                output.push(doc);
                scorer_doc = scorer.advance();
                if scorer_doc == TERMINATED {
                    break;
                }
            }
        }

        Ok(())
    }
}
impl Debug for DocumentQueryEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentQueryEvaluator").finish()
    }
}

/// Segment collector for filter aggregation
pub struct FilterSegmentCollector {
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

impl FilterSegmentCollector {
    /// Create a new filter segment collector following the same pattern as other bucket
    /// aggregations
    pub(crate) fn from_req_and_validate(
        filter_req: &FilterAggregation,
        sub_aggregations: &mut AggregationsWithAccessor,
        segment_reader: &SegmentReader,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        let schema = segment_reader.schema();
        let query = filter_req.parse_query(schema)?;

        let evaluator = DocumentQueryEvaluator::new(query, schema.clone(), segment_reader)?;

        // Follow the same pattern as terms aggregation
        let has_sub_aggregations = !sub_aggregations.is_empty();
        let sub_agg_collector = if has_sub_aggregations {
            // Use the same sub_aggregations structure that will be used at runtime
            // This ensures that the accessor indices match between build-time and runtime
            // Pass the segment_reader to ensure nested filter aggregations also get access
            let sub_aggregation =
                build_segment_agg_collector_with_reader(sub_aggregations, Some(segment_reader))?;
            Some(sub_aggregation)
        } else {
            None
        };

        // Pre-allocate buffer to avoid repeated allocations during collection
        // Use COLLECT_BLOCK_BUFFER_LEN (64) as a reasonable default capacity since:
        // - Documents are processed in blocks of this size
        // - Avoids over-allocation for small segments
        // - Grows automatically if needed for larger batches
        let buffer_capacity = COLLECT_BLOCK_BUFFER_LEN.min(segment_reader.max_doc() as usize);

        Ok(FilterSegmentCollector {
            evaluator,
            doc_count: 0,
            sub_aggregations: sub_agg_collector,
            accessor_idx,
            matching_docs_buffer: Vec::with_capacity(buffer_capacity),
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
        if self.evaluator.matches_document(doc)? {
            self.doc_count += 1;

            // If we have sub-aggregations, collect on them for this filtered document
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];
                sub_aggs.collect(doc, &mut bucket_agg_accessor.sub_aggregation)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        // Use batch filtering for better performance
        self.matching_docs_buffer.clear();
        self.evaluator
            .filter_batch(docs, &mut self.matching_docs_buffer)?;

        self.doc_count += self.matching_docs_buffer.len() as u64;

        // Batch process sub-aggregations if we have matches
        if !self.matching_docs_buffer.is_empty() {
            if let Some(sub_aggs) = &mut self.sub_aggregations {
                let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];
                // Use collect_block for better sub-aggregation performance
                sub_aggs.collect_block(
                    &self.matching_docs_buffer,
                    &mut bucket_agg_accessor.sub_aggregation,
                )?;
            }
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
