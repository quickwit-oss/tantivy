use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::SegmentStatsCollector;

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
/// }
/// ```
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

/// Contains mergeable version of average data.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAverage {
    pub(crate) sum: f64,
    pub(crate) doc_count: u32,
}

impl IntermediateAverage {
    pub(crate) fn from_collector(collector: SegmentStatsCollector) -> Self {
        Self {
            sum: collector.stats.sum,
            doc_count: collector.stats.count,
        }
    }

    /// Merge average data into this instance.
    pub fn merge_fruits(&mut self, other: IntermediateAverage) {
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
}
