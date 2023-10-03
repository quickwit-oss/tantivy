extern crate tuple_vec_map;

use std::collections::BinaryHeap;
use std::fmt::Formatter;

use serde::{Deserialize, Serialize};

use super::{TopHitsMetricResult, TopHitsVecEntry};
use crate::aggregation::bucket::Order;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::{DocAddress, SegmentOrdinal};

/// # Top Hits
///
/// The top hits aggregation is a useful tool to answer questions like:
/// - "What are the most recent posts by each author?"
/// - "What are the most popular items in each category?"
///
/// It does so by keeping track of the most relevant document being aggregated,
/// in terms of a sort criterion that can consist of multiple fields and their
/// sort-orders (ascending or descending).
///
/// `top_hits` should not be used as a top-level aggregation. It is intended to be
/// used as a sub-aggregation, inside a `terms` aggregation or a `filters` aggregation,
/// for example.
///
/// The following example demonstrates a request for the top_hits aggregation:
/// ```JSON
/// {
///     "aggs": {
///         "top_authors": {
///             "terms": {
///                 "field": "author",
///                 "size": 5
///             }
///         },
///         "aggs": {
///             "top_hits": {
///                 "size": 2,
///                 "from": 0
///                 "sort": [
///                     { "date": "desc" }
///                 ]
///             }
///         }
/// }
/// ```
///
/// This request will return an object containing the top two documents, sorted
/// by the `date` field in descending order. You can also sort by multiple fields, which
/// helps to resolve ties. The aggregation object for each bucket will look like:
/// ```JSON
/// {
///     "hits": [
///         {
///           "id": <doc_address>,
///           score: [<time_u64>]
///         },
///         {
///             "id": <doc_address>,
///             score: [<time_u64>]
///         }
///     ]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopHitsAggregation {
    #[serde(with = "tuple_vec_map")]
    sort: Vec<(String, Order)>,
    size: usize,
    from: Option<usize>,
}

impl TopHitsAggregation {
    /// Supposed to return the field name the aggregation is performed on.
    /// This is not implemented yet, because it doesn't make sense for multi-field top hits.
    /// FIXME: Would it make sense to return a `{fields.join(';')}` or something?
    pub fn field_name(&self) -> &str {
        todo!();
    }

    /// Return fields accessed by the aggregator, in order.
    pub fn get_fields(&self) -> Vec<&str> {
        self.sort.iter().map(|(field, _)| field.as_str()).collect()
    }
}

/// Holds a single comparable doc feature, and the order in which it should be sorted.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocFeature {
    /// Stores any u64-mappable feature.
    value: u64,
    /// Sort order for the doc feature
    order: Order,
}

impl Ord for ComparableDocFeature {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.order {
            Order::Asc => self.value.cmp(&other.value),
            Order::Desc => other.value.cmp(&self.value),
        }
    }
}

impl PartialOrd for ComparableDocFeature {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ComparableDocFeature {
    fn eq(&self, other: &Self) -> bool {
        self.value.cmp(&other.value) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableDocFeature {}

/// Multi-feature comparable docs that can be used in a binary heap.
/// Includes a custom `Ord` implementation that:
/// - Allows to sort documents by multiple features
/// - Inverts sorting order to make `BinaryHeap`, a max-heap, behave like a min-heap that lets us
///   access its lowest-scoring member.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocMultipleFeatures {
    doc: DocAddress,
    features: Vec<ComparableDocFeature>,
}

impl Ord for ComparableDocMultipleFeatures {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (self_feature, other_feature) in self.features.iter().zip(other.features.iter()) {
            let cmp = self_feature.cmp(other_feature);
            if cmp != std::cmp::Ordering::Equal {
                return cmp.reverse();
            }
        }
        self.doc.cmp(&other.doc).reverse()
    }
}

impl PartialOrd for ComparableDocMultipleFeatures {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ComparableDocMultipleFeatures {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableDocMultipleFeatures {}

/// The TopHitsCollector used for collecting over segments and merging results.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct TopHitsCollector {
    req: TopHitsAggregation,
    heap: BinaryHeap<ComparableDocMultipleFeatures>,
}

impl std::fmt::Debug for TopHitsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopHitsCollector")
            .field("req", &self.req)
            .field("heap_len", &self.heap.len())
            .finish()
    }
}

impl std::cmp::PartialEq for TopHitsCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl TopHitsCollector {
    fn collect(&mut self, comparable_doc: ComparableDocMultipleFeatures) {
        if self.heap.len() < self.req.size + self.req.from.unwrap_or(0) {
            self.heap.push(comparable_doc);
        } else {
            let mut min_doc = self
                .heap
                .peek_mut()
                .expect("heap should have at least one element at max capacity");
            if comparable_doc < *min_doc {
                *min_doc = comparable_doc;
            }
        }
    }

    pub(crate) fn merge_fruits(&mut self, other_fruit: Self) -> crate::Result<()> {
        for doc in other_fruit.heap.into_vec() {
            self.collect(doc);
        }
        Ok(())
    }

    /// Finalize by converting self into the final result form
    pub fn finalize(self) -> TopHitsMetricResult {
        let mut hits: Vec<TopHitsVecEntry> = self
            .heap
            // TODO: use `into_sorted_iter` once it's stable
            // Using an in-order iterator would avoid allocating a new vector.
            // Ref: https://github.com/rust-lang/rust/issues/59278
            .into_sorted_vec()
            .into_iter()
            .map(|doc| TopHitsVecEntry {
                id: doc.doc,
                sort: doc.features.iter().map(|f| f.value).collect(),
            })
            .collect();

        // Remove the first `from` elements
        // Truncating from end would be more efficient, but we need to truncate from the front
        // because `into_sorted_vec` gives us a descending order because of the inverted
        // `Ord` semantics of the heap elements.
        hits.drain(..self.req.from.unwrap_or(0));
        TopHitsMetricResult { hits }
    }
}

#[derive(Clone)]
pub(crate) struct SegmentTopHitsCollector {
    segment_id: SegmentOrdinal,
    accessor_idx: usize,
    inner_collector: TopHitsCollector,
}

impl SegmentTopHitsCollector {
    // // TODO: confirm if this is required (idts)
    // pub fn new(
    //     segment_id: SegmentOrdinal,
    //     accessor_idx: usize,
    //     req: TopHitsAggregation,
    // ) -> SegmentTopHitsCollector { SegmentTopHitsCollector { segment_id, accessor_idx,
    //   inner_collector: TopHitsCollector { req, heap: BinaryHeap::new(), }, }
    // }

    pub fn from_req(
        req: &TopHitsAggregation,
        accessor_idx: usize,
        segment_id: SegmentOrdinal,
    ) -> Self {
        Self {
            inner_collector: TopHitsCollector {
                req: req.clone(),
                heap: BinaryHeap::with_capacity(req.size + req.from.unwrap_or(0)),
            },
            segment_id,
            accessor_idx,
        }
    }
}

impl std::fmt::Debug for SegmentTopHitsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentTopHitsCollector")
            .field("segment_id", &self.segment_id)
            .field("accessor_idx", &self.accessor_idx)
            .field("inner_collector", &self.inner_collector)
            .finish()
    }
}

impl SegmentAggregationCollector for SegmentTopHitsCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
        results: &mut crate::aggregation::intermediate_agg_result::IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let intermediate_result = IntermediateMetricResult::TopHits(self.inner_collector);
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_result),
        )
    }

    fn collect(
        &mut self,
        doc_id: crate::DocId,
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let features: Vec<ComparableDocFeature> = agg_with_accessor.aggs.values[self.accessor_idx]
            .accessors
            .iter()
            .zip(self.inner_collector.req.sort.iter())
            .map(|((c, _), (_, order))| {
                let order = *order;
                match c.values_for_doc(doc_id).next() {
                    Some(value) => ComparableDocFeature { value, order },
                    // TODO: confirm if this default 0-value is correct
                    None => ComparableDocFeature { value: 0, order },
                }
            })
            .collect();
        self.inner_collector.collect(ComparableDocMultipleFeatures {
            doc: DocAddress {
                doc_id,
                segment_ord: self.segment_id,
            },
            features,
        });
        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        // TODO: Consider getting fields with the column block accessor and refactor this.
        // ---
        // Would the additional complexity of getting fields with the column_block_accessor
        // make sense here? Probably yes, but I want to get a first-pass review first
        // before proceeding.
        for doc in docs {
            self.collect(*doc, agg_with_accessor)?;
        }
        Ok(())
    }
}
