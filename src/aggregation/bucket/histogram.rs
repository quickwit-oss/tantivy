use crate::aggregation::agg_result::BucketResult;
use crate::collector::SegmentCollector;

struct Histogram {}

impl SegmentCollector for Histogram {
    type Fruit = BucketResult;

    fn collect(&mut self, _doc: crate::DocId, _score: crate::Score) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
