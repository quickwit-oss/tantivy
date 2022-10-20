//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;
use std::rc::Rc;
use std::sync::atomic::AtomicU32;

use super::agg_req::MetricAggregation;
use super::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor, MetricAggregationWithAccessor,
};
use super::bucket::{SegmentHistogramCollector, SegmentRangeCollector, SegmentTermCollector};
use super::collector::MAX_BUCKET_COUNT;
use super::intermediate_agg_result::{IntermediateAggregationResults, IntermediateBucketResult};
use super::metric::{
    AverageAggregation, SegmentAverageCollector, SegmentStatsCollector, StatsAggregation,
};
use super::VecWithNames;
use crate::aggregation::agg_req::BucketAggregationType;
use crate::{DocId, TantivyError};

pub(crate) const DOC_BLOCK_SIZE: usize = 64;
pub(crate) type DocBlock = [DocId; DOC_BLOCK_SIZE];

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentAggregationResultsCollector {
    pub(crate) metrics: Option<VecWithNames<SegmentMetricResultCollector>>,
    pub(crate) buckets: Option<VecWithNames<SegmentBucketResultCollector>>,
    staged_docs: DocBlock,
    num_staged_docs: usize,
}

impl Default for SegmentAggregationResultsCollector {
    fn default() -> Self {
        Self {
            metrics: Default::default(),
            buckets: Default::default(),
            staged_docs: [0; DOC_BLOCK_SIZE],
            num_staged_docs: Default::default(),
        }
    }
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
    pub fn into_intermediate_aggregations_result(
        self,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateAggregationResults> {
        let buckets = if let Some(buckets) = self.buckets {
            let entries = buckets
                .into_iter()
                .zip(agg_with_accessor.buckets.values())
                .map(|((key, bucket), acc)| Ok((key, bucket.into_intermediate_bucket_result(acc)?)))
                .collect::<crate::Result<Vec<(String, _)>>>()?;
            Some(VecWithNames::from_entries(entries))
        } else {
            None
        };
        let metrics = self.metrics.map(VecWithNames::from_other);

        Ok(IntermediateAggregationResults { metrics, buckets })
    }

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
    ) -> crate::Result<()> {
        self.staged_docs[self.num_staged_docs] = doc;
        self.num_staged_docs += 1;
        if self.num_staged_docs == self.staged_docs.len() {
            self.flush_staged_docs(agg_with_accessor, false)?;
        }
        Ok(())
    }

    pub(crate) fn flush_staged_docs(
        &mut self,
        agg_with_accessor: &AggregationsWithAccessor,
        force_flush: bool,
    ) -> crate::Result<()> {
        if self.num_staged_docs == 0 {
            return Ok(());
        }
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
                )?;
            }
        }

        self.num_staged_docs = 0;
        Ok(())
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
                avg_collector.collect_block(doc, &*metric.accessor);
            }
            SegmentMetricResultCollector::Stats(stats_collector) => {
                stats_collector.collect_block(doc, &*metric.accessor);
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
    Histogram(Box<SegmentHistogramCollector>),
    Terms(Box<SegmentTermCollector>),
}

impl SegmentBucketResultCollector {
    pub fn into_intermediate_bucket_result(
        self,
        agg_with_accessor: &BucketAggregationWithAccessor,
    ) -> crate::Result<IntermediateBucketResult> {
        match self {
            SegmentBucketResultCollector::Terms(terms) => {
                terms.into_intermediate_bucket_result(agg_with_accessor)
            }
            SegmentBucketResultCollector::Range(range) => {
                range.into_intermediate_bucket_result(agg_with_accessor)
            }
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.into_intermediate_bucket_result(agg_with_accessor)
            }
        }
    }

    pub fn from_req_and_validate(req: &BucketAggregationWithAccessor) -> crate::Result<Self> {
        match &req.bucket_agg {
            BucketAggregationType::Terms(terms_req) => Ok(Self::Terms(Box::new(
                SegmentTermCollector::from_req_and_validate(
                    terms_req,
                    &req.sub_aggregation,
                    req.field_type,
                    req.accessor
                        .as_multi()
                        .expect("unexpected fast field cardinality"),
                )?,
            ))),
            BucketAggregationType::Range(range_req) => {
                Ok(Self::Range(SegmentRangeCollector::from_req_and_validate(
                    range_req,
                    &req.sub_aggregation,
                    &req.bucket_count,
                    req.field_type,
                )?))
            }
            BucketAggregationType::Histogram(histogram) => Ok(Self::Histogram(Box::new(
                SegmentHistogramCollector::from_req_and_validate(
                    histogram,
                    &req.sub_aggregation,
                    req.field_type,
                    req.accessor
                        .as_single()
                        .expect("unexpected fast field cardinality"),
                )?,
            ))),
        }
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        doc: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
        force_flush: bool,
    ) -> crate::Result<()> {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.collect_block(doc, bucket_with_accessor, force_flush)?;
            }
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.collect_block(doc, bucket_with_accessor, force_flush)?;
            }
            SegmentBucketResultCollector::Terms(terms) => {
                terms.collect_block(doc, bucket_with_accessor, force_flush)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct BucketCount {
    /// The counter which is shared between the aggregations for one request.
    pub(crate) bucket_count: Rc<AtomicU32>,
    pub(crate) max_bucket_count: u32,
}

impl Default for BucketCount {
    fn default() -> Self {
        Self {
            bucket_count: Default::default(),
            max_bucket_count: MAX_BUCKET_COUNT,
        }
    }
}

impl BucketCount {
    pub(crate) fn validate_bucket_count(&self) -> crate::Result<()> {
        if self.get_count() > self.max_bucket_count {
            return Err(TantivyError::InvalidArgument(
                "Aborting aggregation because too many buckets were created".to_string(),
            ));
        }
        Ok(())
    }
    pub(crate) fn add_count(&self, count: u32) {
        self.bucket_count
            .fetch_add(count as u32, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn get_count(&self) -> u32 {
        self.bucket_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}
