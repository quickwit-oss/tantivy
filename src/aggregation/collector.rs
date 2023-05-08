use super::agg_req::Aggregations;
use super::agg_req_with_accessor::AggregationsWithAccessor;
use super::agg_result::AggregationResults;
use super::buf_collector::BufAggregationCollector;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::segment_agg_result::{
    build_segment_agg_collector, AggregationLimits, SegmentAggregationCollector,
};
use crate::aggregation::agg_req_with_accessor::get_aggs_with_segment_accessor_and_validate;
use crate::collector::{Collector, SegmentCollector};
use crate::{DocId, SegmentReader, TantivyError};

/// The default max bucket count, before the aggregation fails.
pub const DEFAULT_BUCKET_LIMIT: u32 = 65000;

/// The default memory limit in bytes before the aggregation fails. 500MB
pub const DEFAULT_MEMORY_LIMIT: u64 = 500_000_000;

/// Collector for aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
pub struct AggregationCollector {
    agg: Aggregations,
    limits: AggregationLimits,
}

impl AggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, limits: AggregationLimits) -> Self {
        Self { agg, limits }
    }
}

/// Collector for distributed aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
///
/// # Purpose
/// AggregationCollector returns `IntermediateAggregationResults` and not the final
/// `AggregationResults`, so that results from different indices can be merged and then converted
/// into the final `AggregationResults` via the `into_final_result()` method.
pub struct DistributedAggregationCollector {
    agg: Aggregations,
    limits: AggregationLimits,
}

impl DistributedAggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the limits in `AggregationLimits` is exceeded. (memory limit and
    /// bucket limit)
    pub fn from_aggs(agg: Aggregations, limits: AggregationLimits) -> Self {
        Self { agg, limits }
    }
}

impl Collector for DistributedAggregationCollector {
    type Fruit = IntermediateAggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(&self.agg, reader, &self.limits)
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits)
    }
}

impl Collector for AggregationCollector {
    type Fruit = AggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        AggregationSegmentCollector::from_agg_req_and_reader(&self.agg, reader, &self.limits)
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let res = merge_fruits(segment_fruits)?;
        res.into_final_result(self.agg.clone(), &self.limits)
    }
}

fn merge_fruits(
    mut segment_fruits: Vec<crate::Result<IntermediateAggregationResults>>,
) -> crate::Result<IntermediateAggregationResults> {
    if let Some(fruit) = segment_fruits.pop() {
        let mut fruit = fruit?;
        for next_fruit in segment_fruits {
            fruit.merge_fruits(next_fruit?)?;
        }
        Ok(fruit)
    } else {
        Ok(IntermediateAggregationResults::default())
    }
}

/// `AggregationSegmentCollector` does the aggregation collection on a segment.
pub struct AggregationSegmentCollector {
    aggs_with_accessor: AggregationsWithAccessor,
    agg_collector: BufAggregationCollector,
    error: Option<TantivyError>,
}

impl AggregationSegmentCollector {
    /// Creates an `AggregationSegmentCollector from` an [`Aggregations`] request and a segment
    /// reader. Also includes validation, e.g. checking field types and existence.
    pub fn from_agg_req_and_reader(
        agg: &Aggregations,
        reader: &SegmentReader,
        limits: &AggregationLimits,
    ) -> crate::Result<Self> {
        let mut aggs_with_accessor =
            get_aggs_with_segment_accessor_and_validate(agg, reader, limits)?;
        let result =
            BufAggregationCollector::new(build_segment_agg_collector(&mut aggs_with_accessor)?);
        Ok(AggregationSegmentCollector {
            aggs_with_accessor,
            agg_collector: result,
            error: None,
        })
    }
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = crate::Result<IntermediateAggregationResults>;

    #[inline]
    fn collect(&mut self, doc: DocId, _score: crate::Score) {
        if self.error.is_some() {
            return;
        }
        if let Err(err) = self
            .agg_collector
            .collect(doc, &mut self.aggs_with_accessor)
        {
            self.error = Some(err);
        }
    }

    /// The query pushes the documents to the collector via this method.
    ///
    /// Only valid for Collectors that ignore docs
    fn collect_block(&mut self, docs: &[DocId]) {
        if self.error.is_some() {
            return;
        }
        if let Err(err) = self
            .agg_collector
            .collect_block(docs, &mut self.aggs_with_accessor)
        {
            self.error = Some(err);
        }
    }

    fn harvest(mut self) -> Self::Fruit {
        if let Some(err) = self.error {
            return Err(err);
        }
        self.agg_collector.flush(&mut self.aggs_with_accessor)?;

        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        Box::new(self.agg_collector).add_intermediate_aggregation_result(
            &self.aggs_with_accessor,
            &mut sub_aggregation_res,
        )?;

        Ok(sub_aggregation_res)
    }
}
