use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::*;
use crate::aggregation::*;

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
    /// The missing parameter defines how documents that are missing a value should be treated.
    /// By default they will be ignored but it is also possible to treat them as if they had a
    /// value. Examples in JSON format:
    /// { "field": "my_numbers", "missing": "10.0" }
    #[serde(default, deserialize_with = "deserialize_option_f64")]
    pub missing: Option<f64>,
}

impl SumAggregation {
    /// Creates a new [`SumAggregation`] instance from a field name.
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

/// Intermediate result of the minimum aggregation that can be combined with other intermediate
/// results.
#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntermediateSum {
    stats: IntermediateStats,
}

impl IntermediateSum {
    /// Creates a new [`IntermediateSum`] instance from a [`SegmentStatsCollector`].
    pub(crate) fn from_stats(stats: IntermediateStats) -> Self {
        Self { stats }
    }
    /// Merges the other intermediate result into self.
    pub fn merge_fruits(&mut self, other: IntermediateSum) {
        self.stats.merge_fruits(other.stats);
    }
    /// Computes the final sum value.
    ///
    /// Returns `None` when no values were collected (all documents had
    /// missing/NULL values for the field), matching the behavior of
    /// `IntermediateMin`, `IntermediateMax`, and `IntermediateAvg`.
    pub fn finalize(&self) -> Option<f64> {
        let stats = self.stats.finalize();
        if stats.count == 0 {
            None
        } else {
            Some(stats.sum)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_finalize_returns_none_when_no_values() {
        // Default IntermediateSum has count=0 — finalize should return None,
        // matching MIN/MAX/AVG behavior for all-NULL groups.
        let sum = IntermediateSum::default();
        assert_eq!(sum.finalize(), None);
    }

    #[test]
    fn test_sum_finalize_returns_value_when_has_values() {
        let mut sum = IntermediateSum::default();
        // Merge in a result that has actual values
        let stats = IntermediateStats {
            count: 3,
            sum: 42.0,
            min: 10.0,
            max: 20.0,
            ..Default::default()
        };
        let other = IntermediateSum::from_stats(stats);
        sum.merge_fruits(other);
        assert_eq!(sum.finalize(), Some(42.0));
    }

    #[test]
    fn test_sum_merge_two_empty_still_none() {
        let mut a = IntermediateSum::default();
        let b = IntermediateSum::default();
        a.merge_fruits(b);
        assert_eq!(a.finalize(), None);
    }
}
