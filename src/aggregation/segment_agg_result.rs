//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use itertools::Itertools;

use super::agg_req::MetricAggregation;
use super::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor, BucketAggregationWithAccessor,
    MetricAggregationWithAccessor,
};
use super::bucket::SegmentRangeCollector;
use super::metric::SegmnentAverageCollector;
use super::{Key, VecWithNames};
use crate::aggregation::agg_req::BucketAggregationType;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct SegmentAggregationResultsCollector {
    pub(crate) collectors: VecWithNames<SegmentAggregationResultCollector>,
}

impl SegmentAggregationResultsCollector {
    pub(crate) fn from_req(req: &AggregationsWithAccessor) -> Self {
        SegmentAggregationResultsCollector {
            collectors: VecWithNames::from_entries(
                req.0
                    .entries()
                    .map(|(key, value)| {
                        (
                            key.to_string(),
                            SegmentAggregationResultCollector::from_req(value),
                        )
                    })
                    .collect_vec(),
            ),
        }
    }

    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) {
        for (agg_res, agg_with_accessor) in self
            .collectors
            .values_mut()
            .zip(agg_with_accessor.0.values())
        {
            agg_res.collect(doc, agg_with_accessor);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// TODO Once we have a bench, test if it is helpful to remove the enum here by having two typed
/// vecs in `SegmentAggregationResults`. An aggregation is either a bucket or a metric.
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

    #[inline]
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationWithAccessor,
    ) {
        match self {
            SegmentAggregationResultCollector::Bucket(res) => {
                res.collect(
                    doc,
                    agg_with_accessor
                        .as_bucket()
                        .expect("wrong aggregation type"),
                );
            }
            SegmentAggregationResultCollector::Metric(res) => {
                res.collect(
                    doc,
                    agg_with_accessor
                        .as_metric()
                        .expect("wrong aggregation type"),
                );
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentMetricResultCollector {
    Average(SegmnentAverageCollector),
}

impl SegmentMetricResultCollector {
    pub fn from_req(req: &MetricAggregationWithAccessor) -> Self {
        match &req.metric {
            MetricAggregation::Average { field_name: _ } => SegmentMetricResultCollector::Average(
                SegmnentAverageCollector::from_req(req.field_type),
            ),
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
            BucketAggregationType::RangeAggregation(range_req) => Self::Range(
                SegmentRangeCollector::from_req(range_req, &req.sub_aggregation, req.field_type),
            ),
        }
    }

    #[inline]
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.collect(doc, bucket_with_accessor);
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
    pub sub_aggregation: SegmentAggregationResultsCollector,
}
