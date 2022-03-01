use super::agg_req::Aggregations;
use super::agg_req_with_accessor::AggregationsWithAccessor;
use super::agg_result::AggregationResults;
use super::intermediate_agg_result::IntermediateAggregationResults;
use super::segment_agg_result::SegmentAggregationResultsCollector;
use crate::aggregation::agg_req_with_accessor::get_aggs_with_accessor_and_validate;
use crate::collector::{Collector, SegmentCollector};
use crate::{SegmentReader, TantivyError};

/// Collector for aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
pub struct AggregationCollector {
    agg: Aggregations,
}

impl AggregationCollector {
    /// Create collector from aggregation request.
    pub fn from_aggs(agg: Aggregations) -> Self {
        Self { agg }
    }
}

/// Collector for distributed aggregations.
///
/// The collector collects all aggregations by the underlying aggregation request.
///
/// # Purpose
/// AggregationCollector returns `IntermediateAggregationResults` and not the final
/// `AggregationResults`, so that results from differenct indices can be merged and then converted
/// into the final `AggregationResults` via the `into()` method.
pub struct DistributedAggregationCollector {
    agg: Aggregations,
}

impl DistributedAggregationCollector {
    /// Create collector from aggregation request.
    pub fn from_aggs(agg: Aggregations) -> Self {
        Self { agg }
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
        AggregationSegmentCollector::from_agg_req_and_reader(&self.agg, reader)
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
        let aggs_with_accessor = get_aggs_with_accessor_and_validate(&self.agg, reader)?;
        let result =
            SegmentAggregationResultsCollector::from_req_and_validate(&aggs_with_accessor)?;
        Ok(AggregationSegmentCollector {
            aggs: aggs_with_accessor,
            result,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits).map(|res| res.into())
    }
}

fn merge_fruits(
    mut segment_fruits: Vec<IntermediateAggregationResults>,
) -> crate::Result<IntermediateAggregationResults> {
    if let Some(mut fruit) = segment_fruits.pop() {
        for next_fruit in segment_fruits {
            fruit.merge_fruits(&next_fruit);
        }
        Ok(fruit)
    } else {
        Err(TantivyError::InvalidArgument(
            "no fruits provided in merge_fruits".to_string(),
        ))
    }
}

/// AggregationSegmentCollector does the aggregation collection on a segment.
pub struct AggregationSegmentCollector {
    aggs: AggregationsWithAccessor,
    result: SegmentAggregationResultsCollector,
}

impl AggregationSegmentCollector {
    /// Creates an AggregationSegmentCollector from an [Aggregations] request and a segment reader.
    /// Also includes validation, e.g. checking field types and existence.
    pub fn from_agg_req_and_reader(
        agg: &Aggregations,
        reader: &SegmentReader,
    ) -> crate::Result<Self> {
        let aggs_with_accessor = get_aggs_with_accessor_and_validate(agg, reader)?;
        let result =
            SegmentAggregationResultsCollector::from_req_and_validate(&aggs_with_accessor)?;
        Ok(AggregationSegmentCollector {
            aggs: aggs_with_accessor,
            result,
        })
    }
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = IntermediateAggregationResults;

    #[inline]
    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        self.result.collect(doc, &self.aggs);
    }

    fn harvest(mut self) -> Self::Fruit {
        self.result.flush_staged_docs(&self.aggs, true);
        self.result.into()
    }
}
