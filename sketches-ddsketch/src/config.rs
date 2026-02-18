#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_BINS: u32 = 2048;
const DEFAULT_ALPHA: f64 = 0.01;
const DEFAULT_MIN_VALUE: f64 = 1.0e-9;

/// The configuration struct for constructing a `DDSketch`
#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct Config {
    pub max_num_bins: u32,
    pub gamma: f64,
    pub(crate) gamma_ln: f64,
    pub(crate) min_value: f64,
    pub offset: i32,
}

fn log_gamma(value: f64, gamma_ln: f64) -> f64 {
    value.ln() / gamma_ln
}

impl Config {
    /// Construct a new `Config` struct with specific parameters. If you are unsure of how to
    /// configure this, the `defaults` method constructs a `Config` with built-in defaults.
    ///
    /// `max_num_bins` is the max number of bins the DDSketch will grow to, in steps of 128 bins.
    pub fn new(alpha: f64, max_num_bins: u32, min_value: f64) -> Self {
        // Compute gamma the same way Java's LogarithmicMapping does:
        // gamma = (1 + alpha) / (1 - alpha), which is algebraically
        // the same as 1 + 2*alpha/(1-alpha).
        // Using gamma.ln() (not ln_1p) to match Java's Math.log(gamma)
        // for bit-exact cross-language bin index compatibility.
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let gamma_ln = gamma.ln();

        Config {
            max_num_bins,
            gamma,
            gamma_ln,
            min_value,
            offset: 1 - (log_gamma(min_value, gamma_ln) as i32),
        }
    }

    /// Return a `Config` using built-in default settings
    pub fn defaults() -> Self {
        Self::new(DEFAULT_ALPHA, DEFAULT_MAX_BINS, DEFAULT_MIN_VALUE)
    }

    pub fn key(&self, v: f64) -> i32 {
        // Match Java's LogLikeIndexMapping.index():
        // floor-based indexing for cross-language bin compatibility.
        self.log_gamma(v).floor() as i32
    }

    pub fn value(&self, key: i32) -> f64 {
        // Match Java's LogLikeIndexMapping.value():
        // gamma^key * (1 + relativeAccuracy) = gamma^key * 2*gamma/(gamma+1)
        self.pow_gamma(key) * (2.0 * self.gamma / (1.0 + self.gamma))
    }

    pub fn log_gamma(&self, value: f64) -> f64 {
        log_gamma(value, self.gamma_ln)
    }

    pub fn pow_gamma(&self, key: i32) -> f64 {
        ((key as f64) * self.gamma_ln).exp()
    }

    pub fn min_possible(&self) -> f64 {
        self.min_value
    }

    /// Reconstruct a Config from a gamma value (as decoded from the binary format).
    /// Uses default max_num_bins and min_value.
    pub(crate) fn from_gamma(gamma: f64) -> Self {
        let gamma_ln = gamma.ln();
        Config {
            max_num_bins: DEFAULT_MAX_BINS,
            gamma,
            gamma_ln,
            min_value: DEFAULT_MIN_VALUE,
            offset: 1 - (log_gamma(DEFAULT_MIN_VALUE, gamma_ln) as i32),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(DEFAULT_ALPHA, DEFAULT_MAX_BINS, DEFAULT_MIN_VALUE)
    }
}
