//! Contains aggregation trees which is used during collection in a segment.
//! This tree contains datastructrues optimized for fast collection.
//! The tree can be converted to an intermediate tree, which contains datastructrues optimized for
//! merging.

use std::fmt::Debug;

pub(crate) use super::agg_limits::AggregationLimitsGuard;
use super::intermediate_agg_result::IntermediateAggregationResults;
use crate::aggregation::agg_data::AggregationsData;

/// A SegmentAggregationCollector is used to collect aggregation results.
pub trait SegmentAggregationCollector: CollectorClone + Debug {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_data: &AggregationsData,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()>;

    fn collect(&mut self, doc: crate::DocId, agg_data: &mut AggregationsData) -> crate::Result<()>;

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsData,
    ) -> crate::Result<()>;

    /// Finalize method. Some Aggregator collect blocks of docs before calling `collect_block`.
    /// This method ensures those staged docs will be collected.
    fn flush(&mut self, _agg_data: &mut AggregationsData) -> crate::Result<()> {
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
        self: Box<Self>,
        agg_data: &AggregationsData,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        for agg in self.aggs {
            agg.add_intermediate_aggregation_result(agg_data, results)?;
        }

        Ok(())
    }

    fn collect(&mut self, doc: crate::DocId, agg_data: &mut AggregationsData) -> crate::Result<()> {
        self.collect_block(&[doc], agg_data)?;

        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_data: &mut AggregationsData,
    ) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.collect_block(docs, agg_data)?;
        }

        Ok(())
    }

    fn flush(&mut self, agg_data: &mut AggregationsData) -> crate::Result<()> {
        for collector in &mut self.aggs {
            collector.flush(agg_data)?;
        }
        Ok(())
    }
}
