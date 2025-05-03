use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::{IntermediateStats, SegmentStatsCollector};

/// A single-value metric aggregation that computes the variance of numeric values that are
/// extracted from the aggregated documents.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "variance": {
///         "field": "score"
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VarianceAggregation {
    /// The field name to compute the variance on.
    pub field: String,
}

impl VarianceAggregation {
    /// Creates a new [`VarianceAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self { field: field_name }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Intermediate result of the variance aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateVariance {
    stats: IntermediateStats,
}

impl IntermediateVariance {
    /// Creates a new [`IntermediateVariance`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_collector(collector: SegmentStatsCollector) -> Self {
        Self {
            stats: collector.stats,
        }
    }
    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateVariance) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final variance value.
    pub fn finalize(&self) -> Option<f64> {
        self.stats.finalize().variance
    }
}
