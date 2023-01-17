use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::{IntermediateStats, SegmentStatsCollector};

/// A single-value metric aggregation that computes the maximum of numeric values that are
/// extracted from the aggregated documents.
/// Supported field types are u64, i64, and f64.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "max": {
///         "field": "score",
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MaxAggregation {
    /// The field name to compute the maximum on.
    pub field: String,
}

impl MaxAggregation {
    /// Creates a new [`MaxAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self { field: field_name }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Intermediate result of the maximum aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateMax {
    stats: IntermediateStats,
}

impl IntermediateMax {
    /// Creates a new [`IntermediateMax`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_collector(collector: SegmentStatsCollector) -> Self {
        Self {
            stats: collector.stats,
        }
    }
    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateMax) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final maximum value.
    pub fn finalize(&self) -> Option<f64> {
        self.stats.finalize().max
    }
}
