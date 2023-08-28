use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::{IntermediateStats, SegmentStatsCollector};

/// A single-value metric aggregation that computes the average of numeric values that are
/// extracted from the aggregated documents.
/// See [super::SingleMetricResult] for return value.
///
/// # JSON Format
/// ```json
/// {
///     "avg": {
///         "field": "score"
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AverageAggregation {
    /// The field name to compute the average on.
    pub field: String,
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default)]
    pub missing: Option<f64>,
}

impl AverageAggregation {
    /// Creates a new [`AverageAggregation`] instance from a field name.
    pub fn from_field_name(field_name: String) -> Self {
        Self {
            field: field_name,
            missing: None,
        }
    }
    /// Returns the field name the aggregation is computed on.
    pub fn field_name(&self) -> &str {
        &self.field
    }
}

/// Intermediate result of the average aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateAverage {
    stats: IntermediateStats,
}

impl IntermediateAverage {
    /// Creates a new [`IntermediateAverage`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_collector(collector: SegmentStatsCollector) -> Self {
        Self {
            stats: collector.stats,
        }
    }
    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateAverage) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final average value.
    pub fn finalize(&self) -> Option<f64> {
        self.stats.finalize().avg
    }
}
