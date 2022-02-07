use std::fmt::Debug;

use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};

#[derive(Clone, Default, PartialEq)]
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
    pub(crate) fn collect(&mut self, doc: u32, field: &DynamicFastFieldReader<u64>) {
        let val = field.get(doc);
        self.data.collect(val);
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
    pub fn finalize(&self) -> f64 {
        self.sum as f64 / self.num_vals as f64
    }
    fn collect(&mut self, val: u64) {
        self.num_vals += 1;
        self.sum += val;
    }
}
