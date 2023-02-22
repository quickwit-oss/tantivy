use std::rc::Rc;

use super::agg_req::Aggregations;
use super::agg_req_with_accessor::AggregationsWithAccessor;
use super::agg_result::AggregationResults;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::segment_agg_result::{build_segment_agg_collector, SegmentAggregationCollector};
use crate::aggregation::agg_req_with_accessor::get_aggs_with_accessor_and_validate;
use crate::collector::{Collector, SegmentCollector};
use crate::{SegmentReader, TantivyError};

/// The default max bucket count, before the aggregation fails.
pub const MAX_BUCKET_COUNT: u32 = 65000;

/// Collector for aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
pub struct AggregationCollector {
    agg: Aggregations,
    max_bucket_count: u32,
}

impl AggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// Aggregation fails when the total bucket count is higher than max_bucket_count.
    /// max_bucket_count will default to `MAX_BUCKET_COUNT` (65000) when unset
    pub fn from_aggs(agg: Aggregations, max_bucket_count: Option<u32>) -> Self {
        Self {
            agg,
            max_bucket_count: max_bucket_count.unwrap_or(MAX_BUCKET_COUNT),
        }
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
    max_bucket_count: u32,
}

impl DistributedAggregationCollector {
    /// Create collector from aggregation request.
    ///
    /// max_bucket_count will default to `MAX_BUCKET_COUNT` (65000) when unset
    pub fn from_aggs(agg: Aggregations, max_bucket_count: Option<u32>) -> Self {
        Self {
            agg,
            max_bucket_count: max_bucket_count.unwrap_or(MAX_BUCKET_COUNT),
        }
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
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            self.max_bucket_count,
        )
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
        AggregationSegmentCollector::from_agg_req_and_reader(
            &self.agg,
            reader,
            self.max_bucket_count,
        )
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let res = merge_fruits(segment_fruits)?;
        res.into_final_bucket_result(self.agg.clone())
    }
}

fn merge_fruits(
    mut segment_fruits: Vec<crate::Result<IntermediateAggregationResults>>,
) -> crate::Result<IntermediateAggregationResults> {
    if let Some(fruit) = segment_fruits.pop() {
        let mut fruit = fruit?;
        for next_fruit in segment_fruits {
            fruit.merge_fruits(next_fruit?);
        }
        Ok(fruit)
    } else {
        Ok(IntermediateAggregationResults::default())
    }
}

/// `AggregationSegmentCollector` does the aggregation collection on a segment.
pub struct AggregationSegmentCollector {
    aggs_with_accessor: AggregationsWithAccessor,
    result: Box<dyn SegmentAggregationCollector>,
    error: Option<TantivyError>,
}

impl AggregationSegmentCollector {
    /// Creates an `AggregationSegmentCollector from` an [`Aggregations`] request and a segment
    /// reader. Also includes validation, e.g. checking field types and existence.
    pub fn from_agg_req_and_reader(
        agg: &Aggregations,
        reader: &SegmentReader,
        max_bucket_count: u32,
    ) -> crate::Result<Self> {
        let aggs_with_accessor =
            get_aggs_with_accessor_and_validate(agg, reader, Rc::default(), max_bucket_count)?;
        let result = build_segment_agg_collector(&aggs_with_accessor, true)?;
        Ok(AggregationSegmentCollector {
            aggs_with_accessor,
            result,
            error: None,
        })
    }
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = crate::Result<IntermediateAggregationResults>;

    #[inline]
    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        if self.error.is_some() {
            return;
        }
        if let Err(err) = self.result.collect(doc, &self.aggs_with_accessor) {
            self.error = Some(err);
        }
    }

    fn harvest(mut self) -> Self::Fruit {
        if let Some(err) = self.error {
            return Err(err);
        }
        self.result.flush(&self.aggs_with_accessor)?;
        self.result
            .into_intermediate_aggregations_result(&self.aggs_with_accessor)
    }
}
