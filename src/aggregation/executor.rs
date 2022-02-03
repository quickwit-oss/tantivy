use crate::{
    aggregation::{
        agg_req_with_accessor::get_aggregations_with_accessor,
        bucket::{RangeAggregationReq, SegmentRangeCollector},
        metric::AverageCollector,
        segment_agg_result::{MetricResultCollector, SegmentBucketAggregationResultCollectors},
        BucketAggregationType,
    },
    collector::{Collector, DistributedCollector, SegmentCollector},
    schema::Schema,
};

use super::{
    agg_req::Aggregations,
    agg_req_with_accessor::{AggregationWithAccessor, AggregationsWithAccessor},
    intermediate_agg_result::IntermediateAggregationResultTree,
    segment_agg_result::{SegmentAggregationResultCollector, SegmentAggregationResultTree},
};

struct AggregationCollector {
    agg: Aggregations,
    schema: Schema,
    result: SegmentAggregationResultTree,
}

impl DistributedCollector for AggregationCollector {
    type Fruit = IntermediateAggregationResultTree;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let aggs_with_accessor = get_aggregations_with_accessor(&self.agg, reader);
        todo!()
    }

    fn requires_scoring(&self) -> bool {
        todo!()
    }

    fn merge_fruits(&self, _segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        todo!()
    }
}

impl Collector for AggregationCollector {
    type Fruit = IntermediateAggregationResultTree;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: crate::SegmentOrdinal,
        _segment: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        todo!()
    }

    fn requires_scoring(&self) -> bool {
        todo!()
    }

    fn merge_fruits(&self, _segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        todo!()
    }
}

struct AggregationSegmentCollector {
    aggs: AggregationsWithAccessor,
    schema: Schema,
    result: SegmentAggregationResultTree,
    segment: crate::SegmentReader,
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = IntermediateAggregationResultTree;

    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        for (key, agg_with_accessor) in &self.aggs {
            // Todo prepopulate tree
            let agg_res = self
                .result
                .0
                .entry(key.to_string())
                .or_insert_with(|| get_aggregator(agg_with_accessor));

            agg_res.collect(doc, agg_with_accessor);
        }
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}

fn get_aggregator(agg: &AggregationWithAccessor) -> SegmentAggregationResultCollector {
    match agg {
        AggregationWithAccessor::BucketAggregation(bucket) => match &bucket.bucket_agg {
            BucketAggregationType::TermAggregation { field_name: _ } => todo!(),
            BucketAggregationType::RangeAggregation(req) => {
                let collector = SegmentRangeCollector::from_req(&req);
                SegmentAggregationResultCollector::BucketResult(
                    SegmentBucketAggregationResultCollectors::Range(collector),
                )
            }
        },
        AggregationWithAccessor::MetricAggregation(metric) => match &metric.metric {
            crate::aggregation::MetricAggregation::Average { field_name: _ } => {
                SegmentAggregationResultCollector::MetricResult(MetricResultCollector::Average(
                    AverageCollector::default(),
                ))
            }
        },
    }
}
