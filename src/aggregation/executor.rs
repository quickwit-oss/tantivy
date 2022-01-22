use crate::{
    aggregation::{metric::AverageSegmentAggregator, MetricAggregation},
    collector::SegmentCollector,
    Index,
};

use super::{segment_agg_result::AggregationResultTree, Aggregation};

//fn get_segment_collector(
//agg: Aggregation,
//index: Index,
//) -> Box<dyn SegmentCollector<Fruit = AggregationResultTree>> {
//let schema = index.schema();
//match agg {
//Aggregation::MetricAggregation(MetricAggregation::Average { field_name }) => {
//let fast_field = schema.get_field(&field_name).unwrap();
//AverageSegmentAggregator::new(fast_field)
//}
//Aggregation::BucketAggregation {
//bucket_agg,
//sub_aggregation,
//} => todo!(),
//}
//}

//struct AggregationCollector{}

//impl SegmentCollector for AggregationCollector {

//}
