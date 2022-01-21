use crate::{
    aggregation::SubAggregationCollector,
    collector::SegmentCollector,
    fastfield::{DynamicFastFieldReader, FastFieldReader},
};

struct AverageAggregator {
    sum: u64,
    num_vals: u64,
    fast_field: DynamicFastFieldReader<u64>,
}

impl SegmentCollector for AverageAggregator {
    type Fruit = (u64, u64);

    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        let val = self.fast_field.get(doc);
        self.sum += val;
        self.num_vals += 1;
    }

    fn harvest(self) -> Self::Fruit {
        (self.sum, self.num_vals)
    }
}

impl SubAggregationCollector for AverageAggregator {
    type Fruit = serde_json::Value;

    fn collect(&mut self, result: crate::aggregation::agg_tree::BucketAggregationResult) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
