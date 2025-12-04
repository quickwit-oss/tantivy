//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;

pub(crate) use super::agg_limits::AggregationLimitsGuard;
use super::intermediate_agg_result::IntermediateAggregationResults;
use crate::aggregation::agg_data::AggregationsSegmentCtx;
use crate::aggregation::BucketId;

/// Monotonically increasing provider of BucketIds.
#[derive(Debug, Clone, Default)]
pub struct BucketIdProvider(u32);
impl BucketIdProvider {
    /// Get the next BucketId.
    pub fn next_bucket_id(&mut self) -> BucketId {
        let bucket_id = self.0;
        self.0 += 1;
        bucket_id
    }
}

/// A SegmentAggregationCollector is used to collect aggregation results.
pub trait SegmentAggregationCollector: CollectorClone + Debug {
    fn add_intermediate_aggregation_result(
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()>;

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()>;

    /// Prepare the collector for collecting up to ParentBucketId `max_bucket`.
    /// This is useful so we can split allocation ahead of time of collecting.
    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()>;

    /// Finalize method. Some Aggregator collect blocks of docs before calling `collect_block`.
    /// This method ensures those staged docs will be collected.
    fn flush(&mut self, _agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        Ok(())
    }
}

/// A helper trait to enable cloning of Box<dyn SegmentAggregationCollector>
pub trait CollectorClone {
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
        &mut self,
        agg_data: &AggregationsSegmentCtx,
        results: &mut IntermediateAggregationResults,
        parent_bucket_id: BucketId,
    ) -> crate::Result<()> {
        for agg in &mut self.aggs {
            agg.add_intermediate_aggregation_result(agg_data, results, parent_bucket_id)?;
        }

        Ok(())
    }

    fn collect(
        &mut self,
        parent_bucket_id: BucketId,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.collect(parent_bucket_id, docs, agg_data)?;
        }
        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsSegmentCtx) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.flush(agg_data)?;
        }
        Ok(())
    }

    fn prepare_max_bucket(
        &mut self,
        max_bucket: BucketId,
        agg_data: &AggregationsSegmentCtx,
    ) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.prepare_max_bucket(max_bucket, agg_data)?;
        }
        Ok(())
    }
}
