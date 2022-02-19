//! Module for all metric aggregations.

mod average;
mod stats;
pub use average::*;
use serde::{Deserialize, Serialize};
pub use stats::*;

/// Single-metric aggregations use this common result structure.
///
/// Main reason to wrap it in value is to match elasticsearch output structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SingleMetricResult {
    /// The value of the single value metric.
    pub value: f64,
}

impl From<f64> for SingleMetricResult {
    fn from(value: f64) -> Self {
        Self { value }
    }
}
