use std::{error, fmt};

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::store::Store;

type Result<T> = std::result::Result<T, DDSketchError>;

/// General error type for DDSketch, represents either an invalid quantile or an
/// incompatible merge operation.
#[derive(Debug, Clone)]
pub enum DDSketchError {
    Quantile,
    Merge,
}
impl fmt::Display for DDSketchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DDSketchError::Quantile => {
                write!(f, "Invalid quantile, must be between 0 and 1 (inclusive)")
            }
            DDSketchError::Merge => write!(f, "Can not merge sketches with different configs"),
        }
    }
}
impl error::Error for DDSketchError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic
        None
    }
}

/// This struct represents a [DDSketch](https://arxiv.org/pdf/1908.10693.pdf)
#[derive(Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct DDSketch {
    pub(crate) config: Config,
    pub(crate) store: Store,
    pub(crate) negative_store: Store,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) sum: f64,
    pub(crate) zero_count: u64,
}

impl Default for DDSketch {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

// XXX: functions should return Option<> in the case of empty
impl DDSketch {
    /// Construct a `DDSketch`. Requires a `Config` specifying the parameters of the sketch
    pub fn new(config: Config) -> Self {
        DDSketch {
            config,
            store: Store::new(config.max_num_bins as usize),
            negative_store: Store::new(config.max_num_bins as usize),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            zero_count: 0,
        }
    }

    /// Add the sample to the sketch
    pub fn add(&mut self, v: f64) {
        if v > self.config.min_possible() {
            let key = self.config.key(v);
            self.store.add(key);
        } else if v < -self.config.min_possible() {
            let key = self.config.key(-v);
            self.negative_store.add(key);
        } else {
            self.zero_count += 1;
        }

        if v < self.min {
            self.min = v;
        }
        if self.max < v {
            self.max = v;
        }
        self.sum += v;
    }

    /// Return the quantile value for quantiles between 0.0 and 1.0. Result is an error, represented
    /// as DDSketchError::Quantile if the requested quantile is outside of that range.
    ///
    /// If the sketch is empty the result is None, else Some(v) for the quantile value.
    pub fn quantile(&self, q: f64) -> Result<Option<f64>> {
        if q < 0.0 || q > 1.0 {
            return Err(DDSketchError::Quantile);
        }

        if self.empty() {
            return Ok(None);
        }

        if q == 0.0 {
            return Ok(Some(self.min));
        } else if q == 1.0 {
            return Ok(Some(self.max));
        }

        let rank = (q * (self.count() as f64 - 1.0)) as u64;
        let quantile;
        if rank < self.negative_store.count() {
            let reversed_rank = self.negative_store.count() - rank - 1;
            let key = self.negative_store.key_at_rank(reversed_rank);
            quantile = -self.config.value(key);
        } else if rank < self.zero_count + self.negative_store.count() {
            quantile = 0.0;
        } else {
            let key = self
                .store
                .key_at_rank(rank - self.zero_count - self.negative_store.count());
            quantile = self.config.value(key);
        }

        Ok(Some(quantile))
    }

    /// Returns the minimum value seen, or None if sketch is empty
    pub fn min(&self) -> Option<f64> {
        if self.empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum value seen, or None if sketch is empty
    pub fn max(&self) -> Option<f64> {
        if self.empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns the sum of values seen, or None if sketch is empty
    pub fn sum(&self) -> Option<f64> {
        if self.empty() {
            None
        } else {
            Some(self.sum)
        }
    }

    /// Returns the number of values added to the sketch
    pub fn count(&self) -> usize {
        (self.store.count() + self.zero_count + self.negative_store.count()) as usize
    }

    /// Returns the length of the underlying `Store`. This is mainly only useful for understanding
    /// how much the sketch has grown given the inserted values.
    pub fn length(&self) -> usize {
        self.store.length() as usize + self.negative_store.length() as usize
    }

    /// Merge the contents of another sketch into this one. The sketch that is merged into this one
    /// is unchanged after the merge.
    pub fn merge(&mut self, o: &DDSketch) -> Result<()> {
        if self.config != o.config {
            return Err(DDSketchError::Merge);
        }

        let was_empty = self.store.count() == 0;

        // Merge the stores
        self.store.merge(&o.store);
        self.negative_store.merge(&o.negative_store);
        self.zero_count += o.zero_count;

        // Need to ensure we don't override min/max with initializers
        // if either store were empty
        if was_empty {
            self.min = o.min;
            self.max = o.max;
        } else if o.store.count() > 0 {
            if o.min < self.min {
                self.min = o.min
            }
            if o.max > self.max {
                self.max = o.max;
            }
        }
        self.sum += o.sum;

        Ok(())
    }

    fn empty(&self) -> bool {
        self.count() == 0
    }

    /// Encode this sketch into the Java-compatible binary format used by
    /// `com.datadoghq.sketch.ddsketch.DDSketchWithExactSummaryStatistics`.
    pub fn to_java_bytes(&self) -> Vec<u8> {
        crate::encoding::encode_to_java_bytes(self)
    }

    /// Decode a sketch from the Java-compatible binary format.
    /// Accepts bytes produced by Java's `DDSketchWithExactSummaryStatistics.encode()`
    /// with or without the `0x02` version prefix.
    pub fn from_java_bytes(
        bytes: &[u8],
    ) -> std::result::Result<Self, crate::encoding::DecodeError> {
        crate::encoding::decode_from_java_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{Config, DDSketch};

    #[test]
    fn test_add_zero() {
        let alpha = 0.01;
        let c = Config::new(alpha, 2048, 10e-9);
        let mut dd = DDSketch::new(c);
        dd.add(0.0);
    }

    #[test]
    fn test_quartiles() {
        let alpha = 0.01;
        let c = Config::new(alpha, 2048, 10e-9);
        let mut dd = DDSketch::new(c);

        // Initialize sketch with {1.0, 2.0, 3.0, 4.0}
        for i in 1..5 {
            dd.add(i as f64);
        }

        // We expect the following mappings from quantile to value:
        // [0,0.33]: 1.0, (0.34,0.66]: 2.0, (0.67,0.99]: 3.0, (0.99, 1.0]: 4.0
        let test_cases = vec![
            (0.0, 1.0),
            (0.25, 1.0),
            (0.33, 1.0),
            (0.34, 2.0),
            (0.5, 2.0),
            (0.66, 2.0),
            (0.67, 3.0),
            (0.75, 3.0),
            (0.99, 3.0),
            (1.0, 4.0),
        ];

        for (q, val) in test_cases {
            assert_relative_eq!(dd.quantile(q).unwrap().unwrap(), val, max_relative = alpha);
        }
    }

    #[test]
    fn test_neg_quartiles() {
        let alpha = 0.01;
        let c = Config::new(alpha, 2048, 10e-9);
        let mut dd = DDSketch::new(c);

        // Initialize sketch with {1.0, 2.0, 3.0, 4.0}
        for i in 1..5 {
            dd.add(-i as f64);
        }

        let test_cases = vec![
            (0.0, -4.0),
            (0.25, -4.0),
            (0.5, -3.0),
            (0.75, -2.0),
            (1.0, -1.0),
        ];

        for (q, val) in test_cases {
            assert_relative_eq!(dd.quantile(q).unwrap().unwrap(), val, max_relative = alpha);
        }
    }

    #[test]
    fn test_simple_quantile() {
        let c = Config::defaults();
        let mut dd = DDSketch::new(c);

        for i in 1..101 {
            dd.add(i as f64);
        }

        assert_eq!(dd.quantile(0.95).unwrap().unwrap().ceil(), 95.0);

        assert!(dd.quantile(-1.01).is_err());
        assert!(dd.quantile(1.01).is_err());
    }

    #[test]
    fn test_empty_sketch() {
        let c = Config::defaults();
        let dd = DDSketch::new(c);

        assert_eq!(dd.quantile(0.98).unwrap(), None);
        assert_eq!(dd.max(), None);
        assert_eq!(dd.min(), None);
        assert_eq!(dd.sum(), None);
        assert_eq!(dd.count(), 0);

        assert!(dd.quantile(1.01).is_err());
    }

    #[test]
    fn test_basic_histogram_data() {
        let values = &[
            0.754225035,
            0.752900282,
            0.752812246,
            0.752602367,
            0.754310155,
            0.753525981,
            0.752981082,
            0.752715536,
            0.751667941,
            0.755079054,
            0.753528150,
            0.755188464,
            0.752508723,
            0.750064549,
            0.753960428,
            0.751139298,
            0.752523560,
            0.753253428,
            0.753498342,
            0.751858358,
            0.752104636,
            0.753841300,
            0.754467374,
            0.753814334,
            0.750881719,
            0.753182556,
            0.752576884,
            0.753945708,
            0.753571911,
            0.752314573,
            0.752586651,
        ];

        let c = Config::defaults();
        let mut dd = DDSketch::new(c);

        for value in values {
            dd.add(*value);
        }

        assert_eq!(dd.max(), Some(0.755188464));
        assert_eq!(dd.min(), Some(0.750064549));
        assert_eq!(dd.count(), 31);
        assert_eq!(dd.sum(), Some(23.343630625000003));

        assert!(dd.quantile(0.25).unwrap().is_some());
        assert!(dd.quantile(0.5).unwrap().is_some());
        assert!(dd.quantile(0.75).unwrap().is_some());
    }

    #[test]
    fn test_length() {
        let mut dd = DDSketch::default();
        assert_eq!(dd.length(), 0);

        dd.add(1.0);
        assert_eq!(dd.length(), 128);
        dd.add(2.0);
        dd.add(3.0);
        assert_eq!(dd.length(), 128);

        dd.add(-1.0);
        assert_eq!(dd.length(), 256);
        dd.add(-2.0);
        dd.add(-3.0);
        assert_eq!(dd.length(), 256);
    }
}
