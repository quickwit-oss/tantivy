use std::fmt::Debug;

use crate::aggregation::f64_from_fastfield_u64;
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};
use crate::schema::Type;

#[derive(Clone, PartialEq)]
pub struct SegmnentAverageCollector {
    pub data: AverageData,
    field_type: Type,
}

impl Debug for SegmnentAverageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AverageCollector")
            .field("data", &self.data)
            .finish()
    }
}

impl SegmnentAverageCollector {
    pub fn from_req(field_type: Type) -> Self {
        Self {
            field_type,
            data: Default::default(),
        }
    }
    pub fn merge_fruits(&mut self, other: &SegmnentAverageCollector) {
        self.data.merge_fruits(&other.data);
    }
    pub(crate) fn collect(&mut self, doc: u32, field: &DynamicFastFieldReader<u64>) {
        let val = field.get(doc);
        let val = f64_from_fastfield_u64(val, &self.field_type);
        self.data.collect(val);
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct AverageData {
    pub sum: f64,
    pub num_vals: u64,
}

impl AverageData {
    pub fn from_collector(collector: SegmnentAverageCollector) -> Self {
        collector.data
    }

    pub fn merge_fruits(&mut self, other: &AverageData) {
        self.sum += other.sum;
        self.num_vals += other.num_vals;
    }
    pub fn finalize(&self) -> f64 {
        self.sum / self.num_vals as f64
    }
    #[inline]
    fn collect(&mut self, val: f64) {
        self.num_vals += 1;
        self.sum += val;
    }
}
