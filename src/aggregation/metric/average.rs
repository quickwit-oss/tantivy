use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::aggregation::f64_from_fastfield_u64;
use crate::fastfield::{DynamicFastFieldReader, FastFieldReader};
use crate::schema::Type;
use crate::DocId;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A single-value metric aggregation that computes the average of numeric values that are
/// extracted from the aggregated documents.
/// Supported field types are u64, i64, and f64.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "avg": {
///         "field": "score",
///     }
///  }
///  ```
pub struct AverageAggregation {
    /// The field name to compute the stats on.
    pub field: String,
}
impl AverageAggregation {
    /// Create new AverageAggregation from a field.
    pub fn from_field_name(field_name: String) -> Self {
        AverageAggregation { field: field_name }
    }
    /// Return the field name.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

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
    pub(crate) fn collect_block(&mut self, doc: &[DocId], field: &DynamicFastFieldReader<u64>) {
        let mut iter = doc.chunks_exact(4);
        for docs in iter.by_ref() {
            let val1 = field.get(docs[0]);
            let val2 = field.get(docs[1]);
            let val3 = field.get(docs[2]);
            let val4 = field.get(docs[3]);
            let val1 = f64_from_fastfield_u64(val1, &self.field_type);
            let val2 = f64_from_fastfield_u64(val2, &self.field_type);
            let val3 = f64_from_fastfield_u64(val3, &self.field_type);
            let val4 = f64_from_fastfield_u64(val4, &self.field_type);
            self.data.collect(val1);
            self.data.collect(val2);
            self.data.collect(val3);
            self.data.collect(val4);
        }
        for doc in iter.remainder() {
            let val = field.get(*doc);
            let val = f64_from_fastfield_u64(val, &self.field_type);
            self.data.collect(val);
        }
    }
}

/// Contains mergeable version of average data.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    pub fn finalize(&self) -> Option<f64> {
        if self.doc_count == 0 {
            None
        } else {
            Some(self.sum / self.doc_count as f64)
        }
    }
    #[inline]
    fn collect(&mut self, val: f64) {
        self.doc_count += 1;
        self.sum += val;
    }
}
