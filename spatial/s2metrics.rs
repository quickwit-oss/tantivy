//! S2 cell metrics for determining appropriate cell levels.
//!
//! Ported from s2/s2metrics.h (Eric Veach, Google).

/// S2 cell metrics for determining appropriate cell levels.
pub struct S2Metrics;

impl S2Metrics {
    // kMinWidth.deriv() for quadratic projection: 2 * sqrt(2) / 3
    const MIN_WIDTH_DERIV: f64 = 0.9428090415820634;

    /// Return the maximum level such that the metric is at least the given value, or 0 if there is
    /// no such level.  For example, kMinWidth.GetLevelForMinValue(0.1) returns the maximum level
    /// such that all cells have a minimum width of 0.1 or larger.  The return value is always a
    /// valid level.
    pub fn get_level_for_min_width(value: f64) -> i32 {
        if value <= 0.0 {
            return 30;
        }
        // This code is equivalent to computing a floating-point "level" value and rounding down.
        let ratio = Self::MIN_WIDTH_DERIV / value;
        let level = ratio.log2().floor() as i32;
        level.clamp(0, 30)
    }
}

#[cfg(test)]
#[path = "tests/s2metrics_tests.rs"]
mod tests;
