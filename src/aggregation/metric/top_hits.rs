use std::collections::HashMap;
use std::fmt::Formatter;

use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{TopHitsMetricResult, TopHitsVecEntry};
use crate::aggregation::bucket::Order;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::collector::{ComparableDoc, TopNComputer};
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
    sort: Vec<KeyOrder>,
    size: usize,
    from: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct KeyOrder(String, Order);

impl Serialize for KeyOrder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let KeyOrder(key, order) = self;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(key, order)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for KeyOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let k_o: HashMap<String, Order> = Deserialize::deserialize(deserializer)?;
        let (k, v) = k_o.into_iter().next().ok_or(serde::de::Error::custom(
            "Expected exactly one key-value pair in KeyOrder",
        ))?;
        Ok(Self(k, v))
    }
}

impl TopHitsAggregation {
    /// Return fields accessed by the aggregator, in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.sort
            .iter()
            .map(|KeyOrder(field, _)| field.as_str())
            .collect()
    }
}

/// Holds a single comparable doc feature, and the order in which it should be sorted.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocFeature {
    /// Stores any u64-mappable feature.
    value: Option<u64>,
    /// Sort order for the doc feature
    order: Order,
}

impl Ord for ComparableDocFeature {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let invert = |cmp: std::cmp::Ordering| match self.order {
            Order::Asc => cmp,
            Order::Desc => cmp.reverse(),
        };

        match (self.value, other.value) {
            (Some(self_value), Some(other_value)) => invert(self_value.cmp(&other_value)),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
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

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocFeatures(Vec<ComparableDocFeature>);

impl Ord for ComparableDocFeatures {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (self_feature, other_feature) in self.0.iter().zip(other.0.iter()) {
            let cmp = self_feature.cmp(other_feature);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for ComparableDocFeatures {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ComparableDocFeatures {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableDocFeatures {}

/// The TopHitsCollector used for collecting over segments and merging results.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopHitsCollector {
    req: TopHitsAggregation,
    top_n: TopNComputer<ComparableDocFeatures, DocAddress, false>,
}

impl Default for TopHitsCollector {
    fn default() -> Self {
        Self {
            req: TopHitsAggregation::default(),
            top_n: TopNComputer::new(1),
        }
    }
}

impl std::fmt::Debug for TopHitsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopHitsCollector")
            .field("req", &self.req)
            .field("top_n_threshold", &self.top_n.threshold)
            .finish()
    }
}

impl std::cmp::PartialEq for TopHitsCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl TopHitsCollector {
    fn collect(&mut self, features: ComparableDocFeatures, doc: DocAddress) {
        self.top_n.push(features, doc);
    }

    pub(crate) fn merge_fruits(&mut self, other_fruit: Self) -> crate::Result<()> {
        for doc in other_fruit.top_n.into_vec() {
            self.collect(doc.feature, doc.doc);
        }
        Ok(())
    }

    /// Finalize by converting self into the final result form
    pub fn finalize(self) -> TopHitsMetricResult {
        let mut hits: Vec<TopHitsVecEntry> = self
            .top_n
            .into_sorted_vec()
            .into_iter()
            .map(|doc| TopHitsVecEntry {
                id: doc.doc,
                sort: doc.feature.0.iter().map(|f| f.value).collect(),
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
    pub fn from_req(
        req: &TopHitsAggregation,
        accessor_idx: usize,
        segment_id: SegmentOrdinal,
    ) -> Self {
        Self {
            inner_collector: TopHitsCollector {
                req: req.clone(),
                top_n: TopNComputer::new(req.size + req.from.unwrap_or(0)),
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
            .map(|((c, _), KeyOrder(_, order))| {
                let order = *order;
                ComparableDocFeature {
                    value: c.values_for_doc(doc_id).next(),
                    order,
                }
            })
            .collect();
        self.inner_collector.collect(
            ComparableDocFeatures(features),
            DocAddress {
                segment_ord: self.segment_id,
                doc_id,
            },
        );
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{ComparableDoc, ComparableDocFeature, ComparableDocFeatures, Order};
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::bucket::tests::get_test_index_from_docs;
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::aggregation::AggregationCollector;
    use crate::query::AllQuery;

    fn invert_order(cmp_feature: ComparableDocFeature) -> ComparableDocFeature {
        let ComparableDocFeature { value, order } = cmp_feature;
        let order = match order {
            Order::Asc => Order::Desc,
            Order::Desc => Order::Asc,
        };
        ComparableDocFeature { value, order }
    }

    #[test]
    fn test_comparable_doc_feature() -> crate::Result<()> {
        let small = ComparableDocFeature {
            value: Some(1),
            order: Order::Asc,
        };
        let big = ComparableDocFeature {
            value: Some(2),
            order: Order::Asc,
        };
        let none = ComparableDocFeature {
            value: None,
            order: Order::Asc,
        };

        assert!(small < big);
        assert!(none < small);
        assert!(none < big);

        let small = invert_order(small);
        let big = invert_order(big);
        let none = invert_order(none);

        assert!(small > big);
        assert!(none < small);
        assert!(none < big);

        Ok(())
    }
