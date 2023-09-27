//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;

pub(crate) use super::agg_limits::AggregationLimits;
use super::agg_req::AggregationVariants;
use super::agg_req_with_accessor::{AggregationWithAccessor, AggregationsWithAccessor};
use super::bucket::{SegmentHistogramCollector, SegmentRangeCollector, SegmentTermCollector};
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation,
    SegmentPercentilesCollector, SegmentStatsCollector, SegmentStatsType, StatsAggregation,
    SumAggregation,
};
use crate::aggregation::bucket::TermMissingAgg;

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
    req: &mut AggregationsWithAccessor,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    // Single collector special case
    if req.aggs.len() == 1 {
        let req = &mut req.aggs.values[0];
        let accessor_idx = 0;
        return build_single_agg_segment_collector(req, accessor_idx);
    }

    let agg = GenericSegmentAggregationResultsCollector::from_req_and_validate(req)?;
    Ok(Box::new(agg))
}

pub(crate) fn build_single_agg_segment_collector(
    req: &mut AggregationWithAccessor,
    accessor_idx: usize,
) -> crate::Result<Box<dyn SegmentAggregationCollector>> {
    use AggregationVariants::*;
    match &req.agg.agg {
        Terms(terms_req) => {
            if req.accessors.is_empty() {
                Ok(Box::new(SegmentTermCollector::from_req_and_validate(
                    terms_req,
                    &mut req.sub_aggregation,
                    req.field_type,
                    accessor_idx,
                )?))
            } else {
                Ok(Box::new(TermMissingAgg::new(
                    accessor_idx,
                    &mut req.sub_aggregation,
                )?))
            }
        }
        Range(range_req) => Ok(Box::new(SegmentRangeCollector::from_req_and_validate(
            range_req,
            &mut req.sub_aggregation,
            &req.limits,
            req.field_type,
            accessor_idx,
        )?)),
        Histogram(histogram) => Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
            histogram.clone(),
            &mut req.sub_aggregation,
            req.field_type,
            accessor_idx,
        )?)),
        DateHistogram(histogram) => Ok(Box::new(SegmentHistogramCollector::from_req_and_validate(
            histogram.to_histogram_req()?,
            &mut req.sub_aggregation,
            req.field_type,
            accessor_idx,
        )?)),
        Average(AverageAggregation { missing, .. }) => {
            Ok(Box::new(SegmentStatsCollector::from_req(
                req.field_type,
                SegmentStatsType::Average,
                accessor_idx,
                *missing,
            )))
        }
        Count(CountAggregation { missing, .. }) => Ok(Box::new(SegmentStatsCollector::from_req(
            req.field_type,
            SegmentStatsType::Count,
            accessor_idx,
            *missing,
        ))),
        Max(MaxAggregation { missing, .. }) => Ok(Box::new(SegmentStatsCollector::from_req(
            req.field_type,
            SegmentStatsType::Max,
            accessor_idx,
            *missing,
        ))),
        Min(MinAggregation { missing, .. }) => Ok(Box::new(SegmentStatsCollector::from_req(
            req.field_type,
            SegmentStatsType::Min,
            accessor_idx,
            *missing,
        ))),
        Stats(StatsAggregation { missing, .. }) => Ok(Box::new(SegmentStatsCollector::from_req(
            req.field_type,
            SegmentStatsType::Stats,
            accessor_idx,
            *missing,
        ))),
        Sum(SumAggregation { missing, .. }) => Ok(Box::new(SegmentStatsCollector::from_req(
            req.field_type,
            SegmentStatsType::Sum,
            accessor_idx,
            *missing,
        ))),
        Percentiles(percentiles_req) => Ok(Box::new(
            SegmentPercentilesCollector::from_req_and_validate(
                percentiles_req,
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
    pub(crate) aggs: Vec<Box<dyn SegmentAggregationCollector>>,
}

impl Debug for GenericSegmentAggregationResultsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAggregationResultsCollector")
            .field("aggs", &self.aggs)
            .finish()
    }
}

impl SegmentAggregationCollector for GenericSegmentAggregationResultsCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        for agg in self.aggs {
            agg.add_intermediate_aggregation_result(agg_with_accessor, results)?;
        }

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
        for collector in &mut self.aggs {
            collector.collect_block(docs, agg_with_accessor)?;
        }

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.flush(agg_with_accessor)?;
        }
        Ok(())
    }
}

impl GenericSegmentAggregationResultsCollector {
    pub(crate) fn from_req_and_validate(req: &mut AggregationsWithAccessor) -> crate::Result<Self> {
        let aggs = req
            .aggs
            .values_mut()
            .enumerate()
            .map(|(accessor_idx, req)| build_single_agg_segment_collector(req, accessor_idx))
            .collect::<crate::Result<Vec<Box<dyn SegmentAggregationCollector>>>>()?;

        Ok(GenericSegmentAggregationResultsCollector { aggs })
    }
}
