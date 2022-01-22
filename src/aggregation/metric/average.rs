use crate::{
    aggregation::{agg_result::BucketAggregationResult, SubAggregationCollector},
    collector::{Collector, SegmentCollector},
    fastfield::{DynamicFastFieldReader, FastFieldReader},
    schema::Field,
    SegmentReader,
};

pub(crate) struct AverageSegmentAggregator {
    stats: AverageData,
    fast_field: DynamicFastFieldReader<u64>,
}

impl AverageSegmentAggregator {
    fn new(fast_field: DynamicFastFieldReader<u64>) -> Self {
        AverageSegmentAggregator {
            fast_field,
            stats: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct AverageData {
    sum: u64,
    num_vals: u64,
}

impl AverageData {
    fn finalize(&self) -> f64 {
        self.sum as f64 / self.num_vals as f64
    }
}

impl SegmentCollector for AverageSegmentAggregator {
    type Fruit = AverageData;

    fn collect(&mut self, doc: crate::DocId, _score: crate::Score) {
        let val = self.fast_field.get(doc);
        self.stats.sum += val;
        self.stats.num_vals += 1;
    }

    fn harvest(self) -> Self::Fruit {
        self.stats
    }
}

pub(crate) struct AverageAggregator {
    stats: AverageData,
    field: Field,
}

impl Collector for AverageAggregator {
    type Fruit = AverageData;

    type Child = AverageSegmentAggregator;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<AverageSegmentAggregator> {
        let fast_field_reader = segment_reader.fast_fields().u64(self.field)?;
        Ok(AverageSegmentAggregator::new(fast_field_reader))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_avg_data: Vec<AverageData>) -> crate::Result<AverageData> {
        let mut average_data = AverageData::default();
        for segment_stats in segment_avg_data {
            average_data.sum += segment_stats.sum;
            average_data.num_vals += segment_stats.num_vals;
        }
        Ok(average_data)
    }
}

impl SubAggregationCollector for AverageSegmentAggregator {
    type Fruit = serde_json::Value;

    fn collect(&mut self, _result: BucketAggregationResult) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
