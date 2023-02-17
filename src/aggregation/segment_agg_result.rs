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
use super::buf_collector::BufAggregationCollector;
use super::collector::MAX_BUCKET_COUNT;
use super::intermediate_agg_result::{IntermediateAggregationResults, IntermediateBucketResult};
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation, SegmentStatsCollector,
    SegmentStatsType, StatsAggregation, SumAggregation,
};
use super::VecWithNames;
use crate::aggregation::agg_req::BucketAggregationType;
use crate::{DocId, TantivyError};

pub(crate) trait SegmentAggregationCollector: CollectorClone + Debug {
    fn into_intermediate_aggregations_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateAggregationResults>;

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()>;

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()>;

    /// Finalize method. Some Aggregator collect blocks of docs before calling `collect_block`.
    /// This method ensures those staged docs will be collected.
    fn flush(&mut self, _agg_with_accessor: &AggregationsWithAccessor) -> crate::Result<()> {
        Ok(())
    }
}

pub(crate) trait CollectorClone {
    fn clone_box(&self) -> Box<dyn SegmentAggregationCollector>;
}

impl<T> CollectorClone for T
where T: 'static + SegmentAggregationCollector + Clone
{
    fn clone_box(&self) -> Box<dyn SegmentAggregationCollector> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SegmentAggregationCollector> {
    fn clone(&self) -> Box<dyn SegmentAggregationCollector> {
        self.clone_box()
    }
}

pub(crate) fn build_segment_agg_collector(
    req: &AggregationsWithAccessor,
    add_buffer_layer: bool,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    // Single metric special case
    if req.buckets.is_empty() && req.metrics.len() == 1 {
        let req = &req.metrics.values[0];
        let accessor_idx = 0;
        let stats_collector = match &req.metric {
            MetricAggregation::Average(AverageAggregation { .. }) => {
                SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Average,
                    accessor_idx,
                )
            }
            MetricAggregation::Count(CountAggregation { .. }) => SegmentStatsCollector::from_req(
                req.field_type,
                SegmentStatsType::Count,
                accessor_idx,
            ),
            MetricAggregation::Max(MaxAggregation { .. }) => {
                SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Max, accessor_idx)
            }
            MetricAggregation::Min(MinAggregation { .. }) => {
                SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Min, accessor_idx)
            }
            MetricAggregation::Stats(StatsAggregation { .. }) => SegmentStatsCollector::from_req(
                req.field_type,
                SegmentStatsType::Stats,
                accessor_idx,
            ),
            MetricAggregation::Sum(SumAggregation { .. }) => {
                SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Sum, accessor_idx)
            }
        };

        if add_buffer_layer {
            let stats_collector = BufAggregationCollector::new(stats_collector);
            return Ok(Box::new(stats_collector));
        } else {
            return Ok(Box::new(stats_collector));
        }
    }

    let agg = GenericSegmentAggregationResultsCollector::from_req_and_validate(req)?;
    if add_buffer_layer {
        let agg = BufAggregationCollector::new(agg);
        Ok(Box::new(agg))
    } else {
        Ok(Box::new(agg))
    }
}

#[derive(Clone)]
/// The GenericSegmentAggregationResultsCollector is the generic version of the collector, which
/// can handle arbitrary complexity of  sub-aggregations. Ideally we never have to pick this one
/// and can provide specialized versions instead, that remove some of its overhead.
#[derive(Default)]
pub(crate) struct GenericSegmentAggregationResultsCollector {
    pub(crate) metrics: Option<VecWithNames<SegmentMetricResultCollector>>,
    pub(crate) buckets: Option<VecWithNames<SegmentBucketResultCollector>>,
}

impl Debug for GenericSegmentAggregationResultsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAggregationResultsCollector")
            .field("metrics", &self.metrics)
            .field("buckets", &self.buckets)
            .finish()
    }
}

impl SegmentAggregationCollector for GenericSegmentAggregationResultsCollector {
    fn into_intermediate_aggregations_result(
        self: Box<Self>,
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

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)?;

        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<()> {
        if let Some(metrics) = self.metrics.as_mut() {
            for (collector, agg_with_accessor) in
                metrics.values_mut().zip(agg_with_accessor.metrics.values())
            {
                collector.collect_block(docs, agg_with_accessor);
            }
        }

        if let Some(buckets) = self.buckets.as_mut() {
            for (collector, agg_with_accessor) in
                buckets.values_mut().zip(agg_with_accessor.buckets.values())
            {
                collector.collect_block(docs, agg_with_accessor)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &AggregationsWithAccessor) -> crate::Result<()> {
        if let Some(buckets) = &mut self.buckets {
            for (collector, agg_with_accessor) in
                buckets.values_mut().zip(agg_with_accessor.buckets.values())
            {
                collector.flush(agg_with_accessor)?;
            }
        }
        Ok(())
    }
}

impl GenericSegmentAggregationResultsCollector {
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
            .iter()
            .map(|(key, req)| {
                Ok((
                    key.to_string(),
                    SegmentBucketResultCollector::from_req_and_validate(req)?,
                ))
            })
            .collect::<crate::Result<Vec<(String, _)>>>()?;
        let metrics = req
            .metrics
            .iter()
            .enumerate()
            .map(|(accesor_idx, (key, req))| {
                Ok((
                    key.to_string(),
                    SegmentMetricResultCollector::from_req_and_validate(req, accesor_idx)?,
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
        Ok(GenericSegmentAggregationResultsCollector { metrics, buckets })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentMetricResultCollector {
    Stats(SegmentStatsCollector),
}

impl SegmentMetricResultCollector {
    pub fn from_req_and_validate(
        req: &MetricAggregationWithAccessor,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        match &req.metric {
            MetricAggregation::Average(AverageAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Average,
                    accessor_idx,
                )),
            ),
            MetricAggregation::Count(CountAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Count,
                    accessor_idx,
                )),
            ),
            MetricAggregation::Max(MaxAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Max,
                    accessor_idx,
                )),
            ),
            MetricAggregation::Min(MinAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Min,
                    accessor_idx,
                )),
            ),
            MetricAggregation::Stats(StatsAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Stats,
                    accessor_idx,
                )),
            ),
            MetricAggregation::Sum(SumAggregation { .. }) => Ok(
                SegmentMetricResultCollector::Stats(SegmentStatsCollector::from_req(
                    req.field_type,
                    SegmentStatsType::Sum,
                    accessor_idx,
                )),
            ),
        }
    }
    pub(crate) fn collect_block(&mut self, doc: &[DocId], metric: &MetricAggregationWithAccessor) {
        match self {
            SegmentMetricResultCollector::Stats(stats_collector) => {
                stats_collector.collect_block_with_field(doc, &metric.accessor);
            }
        }
    }
}

/// SegmentBucketAggregationResultCollectors will have specialized buckets for collection inside
/// segments.
/// The typical structure of Map<Key, Bucket> is not suitable during collection for performance
/// reasons.
#[derive(Clone, Debug)]
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
                SegmentTermCollector::from_req_and_validate(terms_req, &req.sub_aggregation)?,
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
                    &req.accessor,
                )?,
            ))),
        }
    }

    #[inline]
    pub(crate) fn collect_block(
        &mut self,
        docs: &[DocId],
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) -> crate::Result<()> {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.collect_block(docs, bucket_with_accessor)?;
            }
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.collect_block(docs, bucket_with_accessor)?;
            }
            SegmentBucketResultCollector::Terms(terms) => {
                terms.collect_block(docs, bucket_with_accessor)?;
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn flush(
        &mut self,
        bucket_with_accessor: &BucketAggregationWithAccessor,
    ) -> crate::Result<()> {
        match self {
            SegmentBucketResultCollector::Range(range) => {
                range.flush(bucket_with_accessor)?;
            }
            SegmentBucketResultCollector::Histogram(histogram) => {
                histogram.flush(bucket_with_accessor)?;
            }
            SegmentBucketResultCollector::Terms(terms) => {
                terms.flush(bucket_with_accessor)?;
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
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn get_count(&self) -> u32 {
        self.bucket_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}
