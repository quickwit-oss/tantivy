use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::{IntermediateStats, SegmentStatsCollector};

/// A single-value metric aggregation that sums up numeric values that are
/// extracted from the aggregated documents.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "sum": {
///         "field": "score"
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SumAggregation {
    /// The field name to compute the minimum on.
    pub field: String,
}

impl SumAggregation {
    /// Creates a new [`SumAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self { field: field_name }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Intermediate result of the minimum aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateSum {
    stats: IntermediateStats,
}

impl IntermediateSum {
    /// Creates a new [`IntermediateSum`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_collector(collector: SegmentStatsCollector) -> Self {
        Self {
            stats: collector.stats,
        }
    }
    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateSum) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final minimum value.
    pub fn finalize(&self) -> Option<f64> {
        Some(self.stats.finalize().sum)
    }
}
