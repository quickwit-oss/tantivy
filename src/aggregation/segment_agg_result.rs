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
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation, SegmentStatsCollector,
    SegmentStatsType, StatsAggregation, SumAggregation,
};
use super::VecWithNames;
use crate::aggregation::agg_req::BucketAggregationType;
use crate::TantivyError;

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
        return build_metric_segment_agg_collector(req, accessor_idx, add_buffer_layer);
    }

    // Single bucket special case
    if req.metrics.is_empty() && req.buckets.len() == 1 {
        let req = &req.buckets.values[0];
        let accessor_idx = 0;
        return build_bucket_segment_agg_collector(req, accessor_idx, add_buffer_layer);
    }

    let agg = GenericSegmentAggregationResultsCollector::from_req_and_validate(req)?;
    if add_buffer_layer {
        let agg = BufAggregationCollector::new(agg);
        Ok(Box::new(agg))
    } else {
        Ok(Box::new(agg))
    }
}

pub(crate) fn build_metric_segment_agg_collector(
    req: &MetricAggregationWithAccessor,
    accessor_idx: usize,
    add_buffer_layer: bool,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    let stats_collector = match &req.metric {
        MetricAggregation::Average(AverageAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Average, accessor_idx)
        }
        MetricAggregation::Count(CountAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Count, accessor_idx)
        }
        MetricAggregation::Max(MaxAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Max, accessor_idx)
        }
        MetricAggregation::Min(MinAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Min, accessor_idx)
        }
        MetricAggregation::Stats(StatsAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Stats, accessor_idx)
        }
        MetricAggregation::Sum(SumAggregation { .. }) => {
            SegmentStatsCollector::from_req(req.field_type, SegmentStatsType::Sum, accessor_idx)
        }
    };

    if add_buffer_layer {
        let stats_collector = BufAggregationCollector::new(stats_collector);
        Ok(Box::new(stats_collector))
    } else {
        Ok(Box::new(stats_collector))
    }
}

fn box_with_opt_buffer<T: SegmentAggregationCollector + Clone + 'static>(
    add_buffer_layer: bool,
    collector: T,
) -> Box<dyn SegmentAggregationCollector> {
    if add_buffer_layer {
        let collector = BufAggregationCollector::new(collector);
        Box::new(collector)
    } else {
        Box::new(collector)
    }
}

pub(crate) fn build_bucket_segment_agg_collector(
    req: &BucketAggregationWithAccessor,
    accessor_idx: usize,
    add_buffer_layer: bool,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    match &req.bucket_agg {
        BucketAggregationType::Terms(terms_req) => Ok(box_with_opt_buffer(
            add_buffer_layer,
            SegmentTermCollector::from_req_and_validate(
                terms_req,
                &req.sub_aggregation,
                accessor_idx,
            )?,
        )),
        BucketAggregationType::Range(range_req) => Ok(box_with_opt_buffer(
            add_buffer_layer,
            SegmentRangeCollector::from_req_and_validate(
                range_req,
                &req.sub_aggregation,
                &req.bucket_count,
                req.field_type,
                accessor_idx,
            )?,
        )),
        BucketAggregationType::Histogram(histogram) => Ok(box_with_opt_buffer(
            add_buffer_layer,
            SegmentHistogramCollector::from_req_and_validate(
                histogram,
                &req.sub_aggregation,
                req.field_type,
                accessor_idx,
            )?,
        )),
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
    fn into_intermediate_aggregations_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateAggregationResults> {
        let buckets = if let Some(buckets) = self.buckets {
            let mut intermeditate_buckets = VecWithNames::default();
            for bucket in buckets {
                // TODO too many allocations?
                let res = bucket.into_intermediate_aggregations_result(agg_with_accessor)?;
                // unwrap is fine since we only have buckets here
                intermeditate_buckets.extend(res.buckets.unwrap());
            }
            Some(intermeditate_buckets)
        } else {
            None
        };
        let metrics = if let Some(metrics) = self.metrics {
            let mut intermeditate_metrics = VecWithNames::default();
            for metric in metrics {
                // TODO too many allocations?
                let res = metric.into_intermediate_aggregations_result(agg_with_accessor)?;
                // unwrap is fine since we only have metrics here
                intermeditate_metrics.extend(res.metrics.unwrap());
            }
            Some(intermeditate_metrics)
        } else {
            None
        };

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

    fn flush(&mut self, agg_with_accessor: &AggregationsWithAccessor) -> crate::Result<()> {
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
                build_bucket_segment_agg_collector(req, accessor_idx, false)
            })
            .collect::<crate::Result<Vec<Box<dyn SegmentAggregationCollector>>>>()?;
        let metrics = req
            .metrics
            .iter()
            .enumerate()
            .map(|(accessor_idx, (_key, req))| {
                build_metric_segment_agg_collector(req, accessor_idx, false)
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
