//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use super::{
    agg_req_with_accessor::{
        AggregationWithAccessor, AggregationsWithAccessor, BucketAggregationWithAccessor,
        MetricAggregationWithAccessor,
    },
    bucket::SegmentRangeCollector,
    executor::get_aggregator,
    metric::AverageCollector,
    Key, MetricAggregation,
};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq)]
// TODO replace HashMap with Vec
// TODO put staged docs here for batch processing, since this is also the top level tree for sub
// aggregations
pub struct SegmentAggregationResults(pub HashMap<String, SegmentAggregationResultCollector>);

impl SegmentAggregationResults {
    pub fn from_req(req: &AggregationsWithAccessor) -> Self {
        SegmentAggregationResults(
            req.iter()
                .map(|(key, agg)| {
                    (
                        key.to_string(),
                        SegmentAggregationResultCollector::from_req(agg),
                    )
                })
                .collect(),
        )
    }

    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) {
        for (key, agg_with_accessor) in agg_with_accessor {
            // TODO prepopulate tree
            let agg_res = self
                .0
                .entry(key.to_string())
                .or_insert_with(|| get_aggregator(agg_with_accessor));

            agg_res.collect(doc, agg_with_accessor);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentAggregationResultCollector {
    Bucket(SegmentBucketResultCollector),
    Metric(SegmentMetricResultCollector),
}

impl SegmentAggregationResultCollector {
    pub fn from_req(req: &AggregationWithAccessor) -> Self {
        match req {
            AggregationWithAccessor::Bucket(bucket) => {
                Self::Bucket(SegmentBucketResultCollector::from_req(bucket))
            }
            AggregationWithAccessor::Metric(metric) => {
                Self::Metric(SegmentMetricResultCollector::from_req(metric))
            }
        }
    }
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationWithAccessor,
    ) {
        match self {
            SegmentAggregationResultCollector::Bucket(res) => {
                res.collect(doc, agg_with_accessor.as_bucket());
            }
            SegmentAggregationResultCollector::Metric(res) => {
                res.collect(doc, agg_with_accessor.as_metric());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentMetricResultCollector {
    Average(AverageCollector),
}

impl SegmentMetricResultCollector {
    pub fn from_req(req: &MetricAggregationWithAccessor) -> Self {
        match &req.metric {
            MetricAggregation::Average { field_name } => {
                SegmentMetricResultCollector::Average(AverageCollector::default())
            }
        }
    }
    pub(crate) fn collect(&mut self, doc: crate::DocId, metric: &MetricAggregationWithAccessor) {
        match self {
            SegmentMetricResultCollector::Average(avg_collector) => {
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
pub enum SegmentBucketResultCollector {
    Range(SegmentRangeCollector),
}

impl SegmentBucketResultCollector {
    pub fn from_req(req: &BucketAggregationWithAccessor) -> Self {
        match &req.bucket_agg {
            super::BucketAggregationType::TermAggregation { field_name } => todo!(),
            super::BucketAggregationType::RangeAggregation(range_req) => Self::Range(
                SegmentRangeCollector::from_req(&range_req, &req.sub_aggregation),
            ),
        }
    }

    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.collect(doc, &bucket_with_accessor);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentBucketDataEntry {
    KeyCount(SegmentBucketDataEntryKeyCount),
}

impl SegmentBucketDataEntry {
    pub fn doc_count(&self) -> u64 {
        match self {
            SegmentBucketDataEntry::KeyCount(bucket) => bucket.doc_count,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentBucketDataEntryKeyCount {
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
    pub sub_aggregation: SegmentAggregationResults,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn get_sub_test_tree(data: &[(String, u64)]) -> SegmentAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                SegmentBucketDataEntry::KeyCount(SegmentBucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: Default::default(),
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
        SegmentAggregationResults(map)
    }

    fn get_test_tree(data: &[(String, u64, String, u64)]) -> SegmentAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                SegmentBucketDataEntry::KeyCount(SegmentBucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )]),
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
        SegmentAggregationResults(map)
    }
}
