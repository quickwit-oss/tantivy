//! An R1Interval represents a closed, bounded interval on the real line. It is capable of
//! representing the empty interval (containing no points) and zero-length intervals (containing a
//! single point).

/// A closed interval on the real line [lo, hi].
///
/// The interval is empty if lo > hi. This is used for latitude bounds and other 1D ranges where
/// wraparound is not needed.
#[derive(Clone, Copy, Debug)]
pub struct R1Interval {
    lo: f64,
    hi: f64,
}

impl PartialEq for R1Interval {
    fn eq(&self, other: &Self) -> bool {
        (self.lo == other.lo && self.hi == other.hi) || (self.is_empty() && other.is_empty())
    }
}

impl R1Interval {
    /// Creates a new interval [lo, hi]. If lo > hi, the interval is empty.
    pub fn new(lo: f64, hi: f64) -> Self {
        Self { lo, hi }
    }

    /// Returns an empty interval. An empty interval contains no points.
    pub fn empty() -> Self {
        Self { lo: 1.0, hi: 0.0 }
    }

    /// Creates an interval containing a single point.
    pub fn from_point(p: f64) -> Self {
        Self { lo: p, hi: p }
    }

    /// Creates the minimal interval containing two points.
    pub fn from_point_pair(p1: f64, p2: f64) -> Self {
        if p1 <= p2 {
            Self { lo: p1, hi: p2 }
        } else {
            Self { lo: p2, hi: p1 }
        }
    }

    /// The low bound of the interval.
    pub fn lo(&self) -> f64 {
        self.lo
    }

    /// The high bound of the interval.
    pub fn hi(&self) -> f64 {
        self.hi
    }

    /// Returns true if the interval is empty (contains no points).
    pub fn is_empty(&self) -> bool {
        self.lo > self.hi
    }

    /// Returns the center of the interval. For empty intervals, the result is arbitrary.
    pub fn center(&self) -> f64 {
        0.5 * (self.lo + self.hi)
    }

    /// Returns the length of the interval. The length of an empty interval is negative.
    pub fn length(&self) -> f64 {
        self.hi - self.lo
    }

    /// Returns true if the given point is in the closed interval [lo, hi].
    pub fn contains_point(&self, p: f64) -> bool {
        p >= self.lo && p <= self.hi
    }

    /// Returns true if the given point is in the open interval (lo, hi). The interior excludes the
    /// endpoints.
    pub fn interior_contains_point(&self, p: f64) -> bool {
        p > self.lo && p < self.hi
    }

    /// Returns true if this interval contains the given interval.
    pub fn contains(&self, other: &R1Interval) -> bool {
        if other.is_empty() {
            true
        } else {
            other.lo >= self.lo && other.hi <= self.hi
        }
    }

    /// Returns true if the interior of this interval contains the entire
    /// interval 'other' (including its boundary).
    pub fn interior_contains(&self, other: &R1Interval) -> bool {
        if other.is_empty() {
            true
        } else {
            other.lo > self.lo && other.hi < self.hi
        }
    }

    /// Returns true if this interval intersects the given interval.
    pub fn intersects(&self, other: &R1Interval) -> bool {
        if self.lo <= other.lo {
            other.lo <= self.hi && other.lo <= other.hi
        } else {
            self.lo <= other.hi && self.lo <= self.hi
        }
    }

    /// Returns true if the interior of this interval intersects any point of the given interval
    /// (including its boundary).
    pub fn interior_intersects(&self, other: &R1Interval) -> bool {
        other.lo < self.hi && self.lo < other.hi && self.lo < self.hi && other.lo <= other.hi
    }

    /// Expands the interval to contain the given point.
    pub fn add_point(&mut self, p: f64) {
        if self.is_empty() {
            self.lo = p;
            self.hi = p;
        } else if p < self.lo {
            self.lo = p;
        } else if p > self.hi {
            self.hi = p;
        }
    }

    /// Expands the interval to contain the given interval.
    pub fn add_interval(&mut self, other: &R1Interval) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = *other;
            return;
        }
        if other.lo < self.lo {
            self.lo = other.lo;
        }
        if other.hi > self.hi {
            self.hi = other.hi;
        }
    }

    /// Returns the closest point in the interval to the given point. The interval must be
    /// non-empty.
    pub fn project(&self, p: f64) -> f64 {
        debug_assert!(!self.is_empty());
        p.clamp(self.lo, self.hi)
    }

    /// Returns an interval expanded by the given margin on each side. If margin is negative,
    /// shrinks instead. Any expansion of an empty interval remains empty.
    pub fn expanded(&self, margin: f64) -> R1Interval {
        if self.is_empty() {
            *self
        } else {
            R1Interval::new(self.lo - margin, self.hi + margin)
        }
    }

    /// Returns the smallest interval containing both this and 'other'.
    pub fn union(&self, other: &R1Interval) -> R1Interval {
        if self.is_empty() {
            *other
        } else if other.is_empty() {
            *self
        } else {
            R1Interval::new(self.lo.min(other.lo), self.hi.max(other.hi))
        }
    }

    /// Returns the intersection of this interval with 'other'. Empty intervals do not need
    /// special-casing.
    pub fn intersection(&self, other: &R1Interval) -> R1Interval {
        R1Interval::new(self.lo.max(other.lo), self.hi.min(other.hi))
    }

    /// Returns true if this interval equals 'other' within the given tolerance. Empty intervals
    /// are considered to match any interval with length <= 2*max_error.
    pub fn approx_equals(&self, other: &R1Interval, max_error: f64) -> bool {
        if self.is_empty() {
            other.length() <= 2.0 * max_error
        } else if other.is_empty() {
            self.length() <= 2.0 * max_error
        } else {
            (other.lo - self.lo).abs() <= max_error && (other.hi - self.hi).abs() <= max_error
        }
    }

    /// Methods that allow the R1Interval to be accessed as a vector.  (The recommended style is to
    /// use lo() and hi() whenever possible, but these methods are useful when the endpoint to be
    /// selected is not constant.)
    pub fn get_bound(&self, i: usize) -> f64 {
        if i == 0 {
            self.lo()
        } else {
            self.hi()
        }
    }

    /// Clamps the given value to this interval.
    #[inline]
    pub fn clamp(&self, v: f64) -> f64 {
        v.max(self.lo()).min(self.hi())
    }
}

impl std::fmt::Display for R1Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.lo, self.hi)
    }
}
