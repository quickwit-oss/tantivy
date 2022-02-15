//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use itertools::Itertools;

use super::agg_req::MetricAggregation;
use super::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor, MetricAggregationWithAccessor,
};
use super::bucket::SegmentRangeCollector;
use super::metric::{
    AverageAggregation, SegmentAverageCollector, SegmentStatsCollector, StatsAggregation,
};
use super::{Key, VecWithNames};
use crate::aggregation::agg_req::BucketAggregationType;
use crate::DocId;

pub(crate) type DocBlock = [DocId; 256];

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SegmentAggregationResultsCollector {
    pub(crate) metrics: VecWithNames<SegmentMetricResultCollector>,
    pub(crate) buckets: VecWithNames<SegmentBucketResultCollector>,
    staged_docs: DocBlock,
    num_staged_docs: usize,
}

impl SegmentAggregationResultsCollector {
    pub(crate) fn from_req(req: &AggregationsWithAccessor) -> crate::Result<Self> {
        let buckets = req
            .buckets
            .entries()
            .map(|(key, req)| {
                Ok((
                    key.to_string(),
                    SegmentBucketResultCollector::from_req(req)?,
                ))
            })
            .collect::<crate::Result<_>>()?;
        let metrics = req
            .metrics
            .entries()
            .map(|(key, req)| (key.to_string(), SegmentMetricResultCollector::from_req(req)))
            .collect_vec();
        Ok(SegmentAggregationResultsCollector {
            metrics: VecWithNames::from_entries(metrics),
            buckets: VecWithNames::from_entries(buckets),
            staged_docs: [0; 256],
            num_staged_docs: 0,
        })
    }

    #[inline]
    pub(crate) fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
        force_flush: bool,
    ) {
        self.staged_docs[self.num_staged_docs] = doc;
        self.num_staged_docs += 1;
        if self.num_staged_docs == self.staged_docs.len() || force_flush {
            self.flush_staged_docs(agg_with_accessor, false);
        }
    }

    #[inline(never)]
    pub(crate) fn flush_staged_docs(
        &mut self,
        agg_with_accessor: &AggregationsWithAccessor,
        force_flush: bool,
    ) {
        for (agg_with_accessor, collector) in agg_with_accessor
            .metrics
            .values()
            .zip(self.metrics.values_mut())
        {
            collector.collect_block(&self.staged_docs[..self.num_staged_docs], agg_with_accessor);
        }
        for (agg_with_accessor, collector) in agg_with_accessor
            .buckets
            .values()
            .zip(self.buckets.values_mut())
        {
            collector.collect_block(
                &self.staged_docs[..self.num_staged_docs],
                agg_with_accessor,
                force_flush,
            );
        }

        self.num_staged_docs = 0;
    }

    //#[inline(never)]
    // pub(crate) fn flush_staged_docs(&mut self, agg_with_accessor: &AggregationsWithAccessor) {
    // for (agg_res, agg_with_accessor) in self
    //.collectors
    //.values_mut()
    //.zip(agg_with_accessor.0.values())
    //{
    // agg_res.collect_block(&self.staged_docs, agg_with_accessor);
    //}
    // self.num_staged_docs = 0;
    //}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentMetricResultCollector {
    Average(SegmentAverageCollector),
    Stats(SegmentStatsCollector),
}

impl SegmentMetricResultCollector {
    pub fn from_req(req: &MetricAggregationWithAccessor) -> Self {
        match &req.metric {
            MetricAggregation::Average(AverageAggregation { field_name: _ }) => {
                SegmentMetricResultCollector::Average(SegmentAverageCollector::from_req(
                    req.field_type,
                ))
            }
            MetricAggregation::Stats(StatsAggregation { field_name: _ }) => {
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(req.field_type))
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
}

impl SegmentBucketResultCollector {
    pub fn from_req(req: &BucketAggregationWithAccessor) -> crate::Result<Self> {
        match &req.bucket_agg {
            BucketAggregationType::Range(range_req) => Ok(Self::Range(
                SegmentRangeCollector::from_req(range_req, &req.sub_aggregation, req.field_type)?,
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
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentBucketEntry {
    KeyCount(SegmentBucketEntryKeyCount),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentBucketEntryKeyCount {
    pub key: Key,
    pub doc_count: u64,
    pub sub_aggregation: SegmentAggregationResultsCollector,
}
