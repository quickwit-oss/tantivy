use crate::aggregation::agg_result::BucketAggregationResult;
use crate::collector::SegmentCollector;

struct Histogram {}

impl SegmentCollector for Histogram {
    type Fruit = BucketAggregationResult;

    fn collect(&mut self, _doc: crate::DocId, _score: crate::Score) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
