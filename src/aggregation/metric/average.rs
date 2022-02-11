use std::fmt::Debug;

use crate::aggregation::f64_from_fastfield_u64;
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};
use crate::schema::Type;

#[derive(Clone, PartialEq)]
pub(crate) struct SegmentAverageCollector {
    pub data: IntermediateAverage,
    field_type: Type,
}

impl Debug for SegmentAverageCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AverageCollector")
            .field("data", &self.data)
            .finish()
    }
}

impl SegmentAverageCollector {
    pub fn from_req(field_type: Type) -> Self {
        Self {
            field_type,
            data: Default::default(),
        }
    }
    pub(crate) fn collect(&mut self, doc: u32, field: &DynamicFastFieldReader<u64>) {
        let val = field.get(doc);
        let val = f64_from_fastfield_u64(val, &self.field_type);
        self.data.collect(val);
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
/// Contains mergeable version of average data.
pub struct IntermediateAverage {
    pub(crate) sum: f64,
    pub(crate) doc_count: u64,
}

impl IntermediateAverage {
    pub(crate) fn from_collector(collector: SegmentAverageCollector) -> Self {
        collector.data
    }

    /// Merge average data into this instance.
    pub fn merge_fruits(&mut self, other: &IntermediateAverage) {
        self.sum += other.sum;
        self.doc_count += other.doc_count;
    }
    /// compute final result
    pub fn finalize(&self) -> f64 {
        self.sum / self.doc_count as f64
    }
    #[inline]
    fn collect(&mut self, val: f64) {
        self.doc_count += 1;
        self.sum += val;
    }
}
