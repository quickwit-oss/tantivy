//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use super::{
    agg_req_with_accessor::{
        AggregationWithAccessor, BucketAggregationWithAccessor, MetricAggregationWithAccessor,
    },
    bucket::SegmentRangeCollector,
    metric::AverageCollector,
    Key,
};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct SegmentAggregationResultTree(pub HashMap<String, SegmentAggregationResultCollector>);

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentAggregationResultCollector {
    BucketResult(SegmentBucketAggregationResultCollectors),
    MetricResult(MetricResultCollector),
}

impl SegmentAggregationResultCollector {
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationWithAccessor,
    ) {
        match self {
            SegmentAggregationResultCollector::BucketResult(res) => {
                res.collect(doc, agg_with_accessor.as_bucket());
            }
            SegmentAggregationResultCollector::MetricResult(res) => {
                res.collect(doc, agg_with_accessor.as_metric());
            }
            _ => {
                panic!("incompatible types in aggregation tree on merge fruits");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetricResultCollector {
    Average(AverageCollector),
}

impl MetricResultCollector {
    pub(crate) fn collect(&mut self, doc: crate::DocId, metric: &MetricAggregationWithAccessor) {
        match self {
            MetricResultCollector::Average(avg_collector) => {
                avg_collector.collect(doc, &metric.accessor);
            }
        }
    }
}

/// SegmentBucketAggregationResultCollectors will have specialized buckets for collection inside
/// segments.
/// The typical structure of Map<Key, Bucket> is not suitable during collection for performance
/// reasons.
#[derive(Clone, Debug, PartialEq)]
pub enum SegmentBucketAggregationResultCollectors {
    Range(SegmentRangeCollector),
}

impl SegmentBucketAggregationResultCollectors {
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        match self {
            SegmentBucketAggregationResultCollectors::Range(range) => {
                range.collect(doc, &bucket_with_accessor.accessor);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BucketDataEntry {
    KeyCount(BucketDataEntryKeyCount),
}

impl BucketDataEntry {
    pub fn doc_count(&self) -> u64 {
        match self {
            BucketDataEntry::KeyCount(bucket) => bucket.doc_count,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct BucketDataEntryKeyCount {
    pub key: Key,
    pub doc_count: u64,
    /// Collect and then compute the values on that bucket.
    /// This is required in cases where we have sub_aggregations.
    ///
    /// For example if we want to calculate the median metric on a bucket, we need to carry all the
    /// values from the SegmentAggregationResultTree to the IntermediateAggregationResultTree, so
    /// that the computation can be done after merging all the segments.
    ///
    /// TODO Handle different data types here?
    /// Collect on Metric level?
    pub values: Option<Vec<u64>>,
    pub sub_aggregation: Option<SegmentAggregationResultTree>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn get_sub_test_tree(data: &[(String, u64)]) -> SegmentAggregationResultTree {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                BucketDataEntry::KeyCount(BucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: None,
                }),
            );
        }
        //map.insert(
        //"my_agg_level2".to_string(),
        //SegmentAggregationResultCollector::BucketResult(
        //SegmentBucketAggregationResultCollector {
        //bucket_agg: BucketAggregationType::TermAggregation {
        //field_name: "field2".to_string(),
        //},
        //buckets,
        //},
        //),
        //);
        SegmentAggregationResultTree(map)
    }

    fn get_test_tree(data: &[(String, u64, String, u64)]) -> SegmentAggregationResultTree {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                BucketDataEntry::KeyCount(BucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: Some(get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )])),
                }),
            );
        }
        //map.insert(
        //"my_agg_level1".to_string(),
        //SegmentAggregationResultCollector::BucketResult(
        //SegmentBucketAggregationResultCollector {
        //bucket_agg: BucketAggregationType::TermAggregation {
        //field_name: "field1".to_string(),
        //},
        //buckets,
        //},
        //),
        //);
        SegmentAggregationResultTree(map)
    }
}
