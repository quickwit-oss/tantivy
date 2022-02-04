use std::fmt::Debug;

use crate::{
    aggregation::agg_result::BucketAggregationResult,
    collector::{Collector, SegmentCollector},
    fastfield::{DynamicFastFieldReader, FastFieldReader},
    schema::Field,
    SegmentReader,
};

// Collector
pub(crate) struct AverageAggregator {
    stats: AverageData,
    field: Field,
}

impl AverageAggregator {
    pub(crate) fn new(field: Field) -> Self {
        AverageAggregator {
            field,
            stats: Default::default(),
        }
    }
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

// SegmentCollector
pub(crate) struct AverageSegmentAggregator {
    stats: AverageData,
    fast_field: DynamicFastFieldReader<u64>,
}

impl AverageSegmentAggregator {
    pub(crate) fn new(fast_field: DynamicFastFieldReader<u64>) -> Self {
        AverageSegmentAggregator {
            fast_field,
            stats: Default::default(),
        }
    }
}

#[derive(Clone, Default)]
pub struct AverageCollector {
    pub data: AverageData,
}

impl Debug for AverageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AverageCollector")
            .field("data", &self.data)
            .finish()
    }
}

impl AverageCollector {
    pub fn merge_fruits(&mut self, other: &AverageCollector) {
        self.data.merge_fruits(&other.data);
    }
    fn finalize(&self) -> f64 {
        self.data.finalize()
    }
    pub(crate) fn collect(&mut self, doc: u32, field: &DynamicFastFieldReader<u64>) {
        let val = field.get(doc);
        self.data.collect(val);
    }
}

impl PartialEq for AverageCollector {
    fn ne(&self, other: &Self) -> bool {
        // equality only on the data
        self.data != other.data
    }
    fn eq(&self, other: &Self) -> bool {
        // equality only on the data
        self.data == other.data
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AverageData {
    pub sum: u64,
    pub num_vals: u64,
}

impl AverageData {
    pub fn from_collector(collector: AverageCollector) -> Self {
        collector.data
    }

    pub fn merge_fruits(&mut self, other: &AverageData) {
        self.sum += other.sum;
        self.num_vals += other.num_vals;
    }
    fn finalize(&self) -> f64 {
        self.sum as f64 / self.num_vals as f64
    }
    fn collect(&mut self, val: u64) {
        self.num_vals += 1;
        self.sum += val;
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
