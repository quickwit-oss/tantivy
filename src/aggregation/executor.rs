use std::collections::HashMap;

use crate::{
    aggregation::{
        agg_tree::BucketAggregation,
        metric::{AverageAggregator, AverageSegmentAggregator},
        MetricAggregation,
    },
    collector::{Collector, DistributedCollector, SegmentCollector},
    schema::Schema,
    Index,
};

use super::{
    agg_result::AggregationResult,
    agg_tree::Aggregations,
    intermediate_agg_result::IntermediateAggregationResultTree,
    segment_agg_result::{SegmentAggregationResultCollector, SegmentAggregationResultTree},
    Aggregation,
};

//fn get_segment_collector(
//agg: Aggregation,
//index: Index,
//) -> Box<dyn SegmentCollector<Fruit = AggregationResultTree>> {
//let schema = index.schema();
//match agg {
//Aggregation::MetricAggregation(MetricAggregation::Average { field_name }) => {
//let fast_field = schema.get_field(&field_name).unwrap();
//AverageSegmentAggregator::new(fast_field);
//}
//Aggregation::BucketAggregation {
//bucket_agg,
//sub_aggregation,
//} => todo!(),
//}
//}

fn root_level_aggs(aggs: Aggregations, schema: Schema, index: Index) {
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let tree = SegmentAggregationResultTree::default();
    for (name, agg) in aggs {
        match agg {
            Aggregation::MetricAggregation(MetricAggregation::Average { field_name }) => {
                let fast_field = schema.get_field(&field_name).unwrap();
                let agg = AverageAggregator::new(fast_field);
            }
            Aggregation::BucketAggregation(BucketAggregation {
                bucket_agg,
                sub_aggregation,
            }) => todo!(),
        }
    }
}

fn sub_aggregation(aggs: Aggregations) {
    for (name, agg) in aggs {}
}

//struct AggregatorExecutionTree {
//tree: HashMap<String, Box<dyn SegmentCollector<Fruit = >>>,
//}

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
        segment_local_id: crate::SegmentOrdinal,
        segment: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        todo!()
    }

    fn requires_scoring(&self) -> bool {
        todo!()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        todo!()
    }
}

impl Collector for AggregationCollector {
    type Fruit = IntermediateAggregationResultTree;

    type Child = AggregationSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        segment: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        todo!()
    }

    fn requires_scoring(&self) -> bool {
        todo!()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> crate::Result<Self::Fruit> {
        todo!()
    }
}

struct AggregationSegmentCollector {
    aggs: Aggregations,
    schema: Schema,
    result: SegmentAggregationResultTree,
    segment: crate::SegmentReader,
}

impl SegmentCollector for AggregationSegmentCollector {
    type Fruit = IntermediateAggregationResultTree;

    fn collect(&mut self, doc: crate::DocId, score: crate::Score) {
        for (key, agg) in &self.aggs {
            // Todo prepopulate tree
            let agg_res = self
                .result
                .0
                .entry(key.to_string())
                .or_insert_with(|| get_aggregator(agg));

            agg_res.collect(doc, &self.segment);
        }
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}

fn get_aggregator(agg: &Aggregation) -> SegmentAggregationResultCollector {
    match agg {
        Aggregation::BucketAggregation(BucketAggregation {
            bucket_agg,
            sub_aggregation,
        }) => todo!(),
        Aggregation::MetricAggregation(_) => todo!(),
    }
}
