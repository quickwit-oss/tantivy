use crate::{aggregation::agg_result::BucketAggregationResult, collector::SegmentCollector};

struct Range {}

impl SegmentCollector for Range {
    type Fruit = BucketAggregationResult;

    fn collect(&mut self, _doc: crate::DocId, _score: crate::Score) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
