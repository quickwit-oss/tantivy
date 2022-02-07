use crate::{
    aggregation::agg_req_with_accessor::get_aggregations_with_accessor,
    collector::{Collector, DistributedCollector, SegmentCollector},
    TantivyError,
};

use super::{
    agg_req::Aggregations, agg_req_with_accessor::AggregationsWithAccessor,
    intermediate_agg_result::IntermediateAggregationResults,
    segment_agg_result::SegmentAggregationResults,
};

/// Collector for aggegations.
///
/// The collector collects all aggregations by the underlying aggregation request.
///
pub struct AggregationCollector {
    agg: Aggregations,
}

impl AggregationCollector {
    /// Create collector from aggegation request.
    pub fn from_aggs(agg: Aggregations) -> Self {
        AggregationCollector { agg }
    }
}

impl DistributedCollector for AggregationCollector {
    type Fruit = IntermediateAggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let aggs_with_accessor = get_aggregations_with_accessor(&self.agg, reader)?;
        let result = SegmentAggregationResults::from_req(&aggs_with_accessor);
        Ok(AggregationSegmentCollector {
            aggs: aggs_with_accessor,
            result,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits)
    }
}

impl Collector for AggregationCollector {
    type Fruit = IntermediateAggregationResults;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let aggs_with_accessor = get_aggregations_with_accessor(&self.agg, reader)?;
        let result = SegmentAggregationResults::from_req(&aggs_with_accessor);
        Ok(AggregationSegmentCollector {
            aggs: aggs_with_accessor,
            result,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        merge_fruits(segment_fruits)
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

pub struct AggregationSegmentCollector {
    aggs: AggregationsWithAccessor,
    result: SegmentAggregationResults,
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = IntermediateAggregationResults;

    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        self.result.collect(doc, &self.aggs);
    }

    fn harvest(self) -> Self::Fruit {
        self.result.into()
    }
}
