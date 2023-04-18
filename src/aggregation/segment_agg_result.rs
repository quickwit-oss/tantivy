//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;

pub(crate) use super::agg_limits::AggregationLimits;
use super::agg_req::MetricAggregation;
use super::agg_req_with_accessor::{
    AggregationsWithAccessor, BucketAggregationWithAccessor, MetricAggregationWithAccessor,
};
use super::bucket::{SegmentHistogramCollector, SegmentRangeCollector, SegmentTermCollector};
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation,
    SegmentPercentilesCollector, SegmentStatsCollector, SegmentStatsType, StatsAggregation,
    SumAggregation,
};
use crate::aggregation::agg_req::BucketAggregationType;

pub(crate) trait SegmentAggregationCollector: CollectorClone + Debug {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()>;

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()>;

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()>;

    /// Finalize method. Some Aggregator collect blocks of docs before calling `collect_block`.
    /// This method ensures those staged docs will be collected.
    fn flush(&mut self, _agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        Ok(())
    }
}

pub(crate) trait CollectorClone {
    fn clone_box(&self) -> Box<dyn SegmentAggregationCollector>;
}

impl<T> CollectorClone for T
where
    T: 'static + SegmentAggregationCollector + Clone,
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
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    // Single metric special case
    if req.buckets.is_empty() && req.metrics.len() == 1 {
        let req = &req.metrics.values[0];
        let accessor_idx = 0;
        return build_metric_segment_agg_collector(req, accessor_idx);
    }

    // Single bucket special case
    if req.metrics.is_empty() && req.buckets.len() == 1 {
        let req = &req.buckets.values[0];
        let accessor_idx = 0;
        return build_bucket_segment_agg_collector(req, accessor_idx);
    }

    let agg = GenericSegmentAggregationResultsCollector::from_req_and_validate(req)?;
    Ok(Box::new(agg))
}

pub(crate) fn build_metric_segment_agg_collector(
    req: &MetricAggregationWithAccessor,
    accessor_idx: usize,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match &req.metric {
        MetricAggregation::Average(AverageAggregation { .. }) => {
            Ok(Box::new(SegmentStatsCollector::from_req(
                req.field_type,
                SegmentStatsType::Average,
                accessor_idx,
            )))
        }
        MetricAggregation::Count(CountAggregation { .. }) => Ok(Box::new(
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Count, accessor_idx),
        )),
        MetricAggregation::Max(MaxAggregation { .. }) => Ok(Box::new(
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Max, accessor_idx),
        )),
        MetricAggregation::Min(MinAggregation { .. }) => Ok(Box::new(
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Min, accessor_idx),
        )),
        MetricAggregation::Stats(StatsAggregation { .. }) => Ok(Box::new(
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Stats, accessor_idx),
        )),
        MetricAggregation::Sum(SumAggregation { .. }) => Ok(Box::new(
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Sum, accessor_idx),
        )),
        MetricAggregation::Percentiles(percentiles_req) => Ok(Box::new(
            SegmentPercentilesCollector::from_req_and_validate(
                percentiles_req,
                req.field_type,
                accessor_idx,
            )?,
        )),
    }
}

pub(crate) fn build_bucket_segment_agg_collector(
    req: &BucketAggregationWithAccessor,
    accessor_idx: usize,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match &req.bucket_agg {
        BucketAggregationType::Terms(terms_req) => {
            Ok(Box::new(SegmentTermCollector::from_req_and_validate(
                terms_req,
                &req.sub_aggregation,
                req.field_type,
                accessor_idx,
            )?))
        }
        BucketAggregationType::Range(range_req) => {
            Ok(Box::new(SegmentRangeCollector::from_req_and_validate(
                range_req,
                &req.sub_aggregation,
                &req.limits,
                req.field_type,
                accessor_idx,
            )?))
        }
        BucketAggregationType::Histogram(histogram) => {
            Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
                histogram,
                &req.sub_aggregation,
                req.field_type,
                accessor_idx,
            )?))
        }
        BucketAggregationType::DateHistogram(histogram) => {
            Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
                &histogram.to_histogram_req()?,
                &req.sub_aggregation,
                req.field_type,
                accessor_idx,
            )?))
        }
    }
}

#[derive(Clone, Default)]
/// The GenericSegmentAggregationResultsCollector is the generic version of the collector, which
/// can handle arbitrary complexity of  sub-aggregations. Ideally we never have to pick this one
/// and can provide specialized versions instead, that remove some of its overhead.
pub(crate) struct GenericSegmentAggregationResultsCollector {
    pub(crate) metrics: Option<Vec<Box<dyn SegmentAggregationCollector>>>,
    pub(crate) buckets: Option<Vec<Box<dyn SegmentAggregationCollector>>>,
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
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        if let Some(buckets) = self.buckets {
            for bucket in buckets {
                bucket.add_intermediate_aggregation_result(agg_with_accessor, results)?;
            }
        };
        if let Some(metrics) = self.metrics {
            for metric in metrics {
                metric.add_intermediate_aggregation_result(agg_with_accessor, results)?;
            }
        };

        Ok(())
    }

    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)?;

        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        if let Some(metrics) = self.metrics.as_mut() {
            for collector in metrics {
                collector.collect_block(docs, agg_with_accessor)?;
            }
        }

        if let Some(buckets) = self.buckets.as_mut() {
            for collector in buckets {
                collector.collect_block(docs, agg_with_accessor)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        if let Some(metrics) = &mut self.metrics {
            for collector in metrics {
                collector.flush(agg_with_accessor)?;
            }
        }
        if let Some(buckets) = &mut self.buckets {
            for collector in buckets {
                collector.flush(agg_with_accessor)?;
            }
        }
        Ok(())
    }
}

impl GenericSegmentAggregationResultsCollector {
    pub(crate) fn from_req_and_validate(req: &AggregationsWithAccessor) -> crate::Result<Self> {
        let buckets = req
            .buckets
            .iter()
            .enumerate()
            .map(|(accessor_idx, (_key, req))| {
                build_bucket_segment_agg_collector(req, accessor_idx)
            })
            .collect::<crate::Result<Vec<Box<dyn SegmentAggregationCollector>>>>()?;
        let metrics = req
            .metrics
            .iter()
            .enumerate()
            .map(|(accessor_idx, (_key, req))| {
                build_metric_segment_agg_collector(req, accessor_idx)
            })
            .collect::<crate::Result<Vec<Box<dyn SegmentAggregationCollector>>>>()?;

        let metrics = if metrics.is_empty() {
            None
        } else {
            Some(metrics)
        };

        let buckets = if buckets.is_empty() {
            None
        } else {
            Some(buckets)
        };
        Ok(GenericSegmentAggregationResultsCollector { metrics, buckets })
    }
}
