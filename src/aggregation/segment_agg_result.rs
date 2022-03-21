//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;

use super::agg_req::MetricAggregation;
use super::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor, MetricAggregationWithAccessor,
};
use super::bucket::{SegmentHistogramCollector, SegmentRangeCollector};
use super::metric::{
    AverageAggregation, SegmentAverageCollector, SegmentStatsCollector, StatsAggregation,
};
use super::{Key, VecWithNames};
use crate::aggregation::agg_req::BucketAggregationType;
use crate::DocId;

pub(crate) const DOC_BLOCK_SIZE: usize = 64;
pub(crate) type DocBlock = [DocId; DOC_BLOCK_SIZE];

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentAggregationResultsCollector {
    pub(crate) metrics: Option<VecWithNames<SegmentMetricResultCollector>>,
    pub(crate) buckets: Option<VecWithNames<SegmentBucketResultCollector>>,
    staged_docs: DocBlock,
    num_staged_docs: usize,
}

impl Debug for SegmentAggregationResultsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAggregationResultsCollector")
            .field("metrics", &self.metrics)
            .field("buckets", &self.buckets)
            .field("staged_docs", &&self.staged_docs[..self.num_staged_docs])
            .field("num_staged_docs", &self.num_staged_docs)
            .finish()
    }
}

impl SegmentAggregationResultsCollector {
    pub(crate) fn from_req_and_validate(req: &AggregationsWithAccessor) -> crate::Result<Self> {
        let buckets = req
            .buckets
            .entries()
            .map(|(key, req)| {
                Ok((
                    key.to_string(),
                    SegmentBucketResultCollector::from_req_and_validate(req)?,
                ))
            })
            .collect::<crate::Result<Vec<(String, _)>>>()?;
        let metrics = req
            .metrics
            .entries()
            .map(|(key, req)| {
                Ok((
                    key.to_string(),
                    SegmentMetricResultCollector::from_req_and_validate(req)?,
                ))
            })
            .collect::<crate::Result<Vec<(String, _)>>>()?;
        let metrics = if metrics.is_empty() {
            None
        } else {
            Some(VecWithNames::from_entries(metrics))
        };
        let buckets = if buckets.is_empty() {
            None
        } else {
            Some(VecWithNames::from_entries(buckets))
        };
        Ok(SegmentAggregationResultsCollector {
            metrics,
            buckets,
            staged_docs: [0; DOC_BLOCK_SIZE],
            num_staged_docs: 0,
        })
    }

    #[inline]
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) {
        self.staged_docs[self.num_staged_docs] = doc;
        self.num_staged_docs += 1;
        if self.num_staged_docs == self.staged_docs.len() {
            self.flush_staged_docs(agg_with_accessor, false);
        }
    }

    pub(crate) fn flush_staged_docs(
        &mut self,
        agg_with_accessor: &AggregationsWithAccessor,
        force_flush: bool,
    ) {
        if let Some(metrics) = &mut self.metrics {
            for (collector, agg_with_accessor) in
                metrics.values_mut().zip(agg_with_accessor.metrics.values())
            {
                collector
                    .collect_block(&self.staged_docs[..self.num_staged_docs], agg_with_accessor);
            }
        }

        if let Some(buckets) = &mut self.buckets {
            for (collector, agg_with_accessor) in
                buckets.values_mut().zip(agg_with_accessor.buckets.values())
            {
                collector.collect_block(
                    &self.staged_docs[..self.num_staged_docs],
                    agg_with_accessor,
                    force_flush,
                );
            }
        }

        self.num_staged_docs = 0;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentMetricResultCollector {
    Average(SegmentAverageCollector),
    Stats(SegmentStatsCollector),
}

impl SegmentMetricResultCollector {
    pub fn from_req_and_validate(req: &MetricAggregationWithAccessor) -> crate::Result<Self> {
        match &req.metric {
            MetricAggregation::Average(AverageAggregation { field: _ }) => {
                Ok(SegmentMetricResultCollector::Average(
                    SegmentAverageCollector::from_req(req.field_type),
                ))
            }
            MetricAggregation::Stats(StatsAggregation { field: _ }) => {
                Ok(SegmentMetricResultCollector::Stats(
                    SegmentStatsCollector::from_req(req.field_type),
                ))
            }
        }
    }
    pub(crate) fn collect_block(&mut self, doc: &[DocId], metric: &MetricAggregationWithAccessor) {
        match self {
            SegmentMetricResultCollector::Average(avg_collector) => {
                avg_collector.collect_block(doc, &metric.accessor);
            }
            SegmentMetricResultCollector::Stats(stats_collector) => {
                stats_collector.collect_block(doc, &metric.accessor);
            }
        }
    }
}

/// SegmentBucketAggregationResultCollectors will have specialized buckets for collection inside
/// segments.
/// The typical structure of Map<Key, Bucket> is not suitable during collection for performance
/// reasons.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentBucketResultCollector {
    Range(SegmentRangeCollector),
    Histogram(SegmentHistogramCollector),
}

impl SegmentBucketResultCollector {
    pub fn from_req_and_validate(req: &BucketAggregationWithAccessor) -> crate::Result<Self> {
        match &req.bucket_agg {
            BucketAggregationType::Range(range_req) => {
                Ok(Self::Range(SegmentRangeCollector::from_req_and_validate(
                    range_req,
                    &req.sub_aggregation,
                    req.field_type,
                )?))
            }
            BucketAggregationType::Histogram(histogram) => Ok(Self::Histogram(
                SegmentHistogramCollector::from_req_and_validate(
                    histogram,
                    &req.sub_aggregation,
                    req.field_type,
                    &req.accessor,
                )?,
            )),
        }
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        doc: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
        force_flush: bool,
    ) {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.collect_block(doc, bucket_with_accessor, force_flush);
            }
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.collect_block(doc, bucket_with_accessor, force_flush)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentHistogramBucketEntry {
    pub key: f64,
    pub doc_count: u64,
}

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentRangeBucketEntry {
    pub key: Key,
    pub doc_count: u64,
    pub sub_aggregation: Option<SegmentAggregationResultsCollector>,
    /// The from range of the bucket. Equals f64::MIN when None.
    pub from: Option<f64>,
    /// The to range of the bucket. Equals f64::MAX when None.
    pub to: Option<f64>,
}

impl Debug for SegmentRangeBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentRangeBucketEntry")
            .field("key", &self.key)
            .field("doc_count", &self.doc_count)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish()
    }
}
