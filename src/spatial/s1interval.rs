//! An S1Interval represents a closed interval on a unit circle (also known as a 1-dimensional
//! sphere).  It is capable of representing the empty interval (containing no points), the full
//! interval (containing all points), and zero-length intervals (containing a single point).
//!
//! Points are represented by the angle they make with the positive x-axis in! the range [-Pi, Pi].
//! An interval is represented by its lower and upper bounds (both inclusive, since the interval is
//! closed).  The lower bound may be greater than the upper bound, in which case the interval is
//! "inverted" (i.e. it passes through the point (-1, 0)).
//!
//! Note that the point (-1, 0) has two valid representations, Pi and -Pi. The normalized
//! representation of this point internally is Pi, so that endpoints of normal intervals are in the
//! range (-Pi, Pi].  However, we take advantage of the point -Pi to construct two special
//! intervals: the Full() interval is [-Pi, Pi], and the Empty() interval is [Pi, -Pi].
//!
//! S1Interval in s1interval.h and s1interval.cc

use std::f64::consts::PI;

use super::math::remainder;

/// A closed interval on the unit circle.
///
/// Points are represented by angles in the range [-PI, PI]. The interval is represented by its
/// lower and upper bounds (both inclusive). The lower bound may be greater than the upper bound,
/// in which case the interval is "inverted" (it passes through the point at PI/-PI).
///
/// Special intervals:
/// - Empty: `[PI, -PI]` contains no points
/// - Full: `[-PI, PI]` contains all points
#[derive(Clone, Copy, Debug)]
pub struct S1Interval {
    lo: f64,
    hi: f64,
}

impl S1Interval {
    /// Creates a new interval with the given bounds.
    ///
    /// Both endpoints must be in the range [-PI, PI]. The value -PI is normalized to PI except for
    /// the Full and Empty intervals.
    pub fn new(lo: f64, hi: f64) -> Self {
        let mut result = Self { lo, hi };
        if lo == -PI && hi != PI {
            result.lo = PI;
        }
        if hi == -PI && lo != PI {
            result.hi = PI;
        }
        debug_assert!(result.is_valid());
        result
    }

    /// Internal constructor that assumes bounds are already normalized.
    fn new_checked(lo: f64, hi: f64) -> Self {
        let result = Self { lo, hi };
        debug_assert!(result.is_valid());
        result
    }

    /// Returns the empty interval (contains no points).
    pub fn empty() -> Self {
        Self { lo: PI, hi: -PI }
    }

    /// Returns the full interval (contains all points).
    pub fn full() -> Self {
        Self { lo: -PI, hi: PI }
    }

    /// Creates an interval containing a single point.
    pub fn from_point(mut p: f64) -> Self {
        if p == -PI {
            p = PI;
        }
        Self::new_checked(p, p)
    }

    /// Creates the minimal interval containing two points. This is equivalent to starting with an
    /// empty interval and calling `add_point()` twice, but more efficient.
    pub fn from_point_pair(mut p1: f64, mut p2: f64) -> Self {
        debug_assert!(p1.abs() <= PI);
        debug_assert!(p2.abs() <= PI);
        if p1 == -PI {
            p1 = PI;
        }
        if p2 == -PI {
            p2 = PI;
        }
        if positive_distance(p1, p2) <= PI {
            Self::new_checked(p1, p2)
        } else {
            Self::new_checked(p2, p1)
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

    /// Returns true if the interval is valid. An interval is valid if neither bound exceeds PI in
    /// absolute value, and the value -PI appears only in the Empty and Full intervals.
    pub fn is_valid(&self) -> bool {
        self.lo.abs() <= PI
            && self.hi.abs() <= PI
            && !(self.lo == -PI && self.hi != PI)
            && !(self.hi == -PI && self.lo != PI)
    }

    /// Returns true if the interval contains all points on the circle.
    pub fn is_full(&self) -> bool {
        self.lo == -PI && self.hi == PI
    }

    /// Returns true if the interval is empty.
    pub fn is_empty(&self) -> bool {
        self.lo == PI && self.hi == -PI
    }

    /// Returns true if lo > hi (the interval wraps around PI/-PI).
    pub fn is_inverted(&self) -> bool {
        self.lo > self.hi
    }

    /// Returns the midpoint of the interval. For full and empty intervals, the result is
    /// arbitrary.
    pub fn center(&self) -> f64 {
        let center = 0.5 * (self.lo + self.hi);
        if !self.is_inverted() {
            center
        } else if center <= 0.0 {
            center + PI
        } else {
            center - PI
        }
    }

    /// Returns the length of the interval. The length of an empty interval is negative.
    pub fn length(&self) -> f64 {
        let mut length = self.hi - self.lo;
        if length >= 0.0 {
            return length;
        }
        length += 2.0 * PI;
        // Empty intervals have a negative length.
        if length > 0.0 {
            length
        } else {
            -1.0
        }
    }

    /// Returns the complement of the interior of the interval. An interval and its complement have
    /// the same boundary but do not share any interior values.
    pub fn complement(&self) -> Self {
        if self.lo == self.hi {
            // Singleton.
            Self::full()
        } else {
            // Handles empty and full correctly.
            Self::new_checked(self.hi, self.lo)
        }
    }

    /// Returns the midpoint of the complement of the interval. For full and empty intervals, the
    /// result is arbitrary. For a singleton interval, returns its antipodal point on S1.
    pub fn complement_center(&self) -> f64 {
        if self.lo != self.hi {
            self.complement().center()
        } else {
            // Singleton.
            if self.hi <= 0.0 {
                self.hi + PI
            } else {
                self.hi - PI
            }
        }
    }

    /// Returns true if the interval contains the given point. Return true if the interval (which
    /// is closed) contains the point 'p'. Skips the normalization of 'p' from -Pi to Pi.
    fn fast_contains(&self, p: f64) -> bool {
        if self.is_inverted() {
            (p >= self.lo || p <= self.hi) && !self.is_empty()
        } else {
            p >= self.lo && p <= self.hi
        }
    }

    /// Returns true if the interval contains the given point. The point must be in the range
    /// [-PI, PI].
    pub fn contains_point(&self, mut p: f64) -> bool {
        debug_assert!(p.abs() <= PI);
        if p == -PI {
            p = PI;
        }
        self.fast_contains(p)
    }

    /// Returns true if the interior of the interval contains the point.
    pub fn interior_contains_point(&self, mut p: f64) -> bool {
        debug_assert!(p.abs() <= PI);
        if p == -PI {
            p = PI;
        }
        if self.is_inverted() {
            p > self.lo || p < self.hi
        } else {
            (p > self.lo && p < self.hi) || self.is_full()
        }
    }

    /// Returns true if this interval contains the given interval. Works for empty, full, and
    /// singleton intervals.
    pub fn contains(&self, y: &S1Interval) -> bool {
        if self.is_inverted() {
            if y.is_inverted() {
                y.lo >= self.lo && y.hi <= self.hi
            } else {
                (y.lo >= self.lo || y.hi <= self.hi) && !self.is_empty()
            }
        } else if y.is_inverted() {
            self.is_full() || y.is_empty()
        } else {
            y.lo >= self.lo && y.hi <= self.hi
        }
    }

    /// Returns true if the interior of this interval contains the entire given interval.  Note
    /// that x.InteriorContains(x) is true only when x is the empty or full interval, and
    /// x.InteriorContains(S1Interval(p,p)) is equivalent to x.InteriorContains(p).
    pub fn interior_contains(&self, y: &S1Interval) -> bool {
        if self.is_inverted() {
            if !y.is_inverted() {
                y.lo > self.lo || y.hi < self.hi
            } else {
                (y.lo > self.lo && y.hi < self.hi) || y.is_empty()
            }
        } else if y.is_inverted() {
            self.is_full() || y.is_empty()
        } else {
            (y.lo > self.lo && y.hi < self.hi) || self.is_full()
        }
    }

    /// Return true if the two intervals contain any points in common. Note that the point +/-Pi
    /// has two representations, so the intervals [-Pi,-3] and [2,Pi] intersect, for example.
    pub fn intersects(&self, y: &S1Interval) -> bool {
        if self.is_empty() || y.is_empty() {
            return false;
        }
        if self.is_inverted() {
            // Every non-empty inverted interval contains PI.
            y.is_inverted() || y.lo <= self.hi || y.hi >= self.lo
        } else if y.is_inverted() {
            y.lo <= self.hi || y.hi >= self.lo
        } else {
            y.lo <= self.hi && y.hi >= self.lo
        }
    }

    /// Returns true if the interior of this interval intersects the given interval. Works for
    /// empty, full, and singleton intervals.
    pub fn interior_intersects(&self, y: &S1Interval) -> bool {
        if self.is_empty() || y.is_empty() || self.lo == self.hi {
            return false;
        }
        if self.is_inverted() {
            y.is_inverted() || y.lo < self.hi || y.hi > self.lo
        } else if y.is_inverted() {
            y.lo < self.hi || y.hi > self.lo
        } else {
            (y.lo < self.hi && y.hi > self.lo) || self.is_full()
        }
    }

    /// Expands the interval to contain the given point. The point must be in the range [-PI, PI].
    pub fn add_point(&mut self, mut p: f64) {
        debug_assert!(p.abs() <= PI);
        if p == -PI {
            p = PI;
        }

        if self.fast_contains(p) {
            return;
        }
        if self.is_empty() {
            self.lo = p;
            self.hi = p;
        } else {
            // Compute distance from p to each endpoint.
            let dlo = positive_distance(p, self.lo);
            let dhi = positive_distance(self.hi, p);
            if dlo < dhi {
                self.lo = p;
            } else {
                self.hi = p;
            }
            // Adding a point can never turn a non-full interval into a full one.
        }
    }

    /// Returns the closest point in the interval to the given point. The interval must be
    /// non-empty.
    pub fn project(&self, mut p: f64) -> f64 {
        debug_assert!(!self.is_empty());
        debug_assert!(p.abs() <= PI);
        if p == -PI {
            p = PI;
        }
        if self.fast_contains(p) {
            return p;
        }
        // Return the endpoint that is closer.
        let dlo = positive_distance(p, self.lo);
        let dhi = positive_distance(self.hi, p);
        if dlo < dhi {
            self.lo
        } else {
            self.hi
        }
    }

    /// Returns an interval expanded by the given margin on each side. If margin is negative,
    /// shrinks instead. The result may be empty or full. Expansion of a full interval remains
    /// full; expansion of an empty interval remains empty.
    pub fn expanded(&self, margin: f64) -> S1Interval {
        if margin >= 0.0 {
            if self.is_empty() {
                return *self;
            }
            // Check if this interval will be full after expansion. Allow for 1-bit rounding error.
            if self.length() + 2.0 * margin + 2.0 * f64::EPSILON >= 2.0 * PI {
                return S1Interval::full();
            }
        } else {
            if self.is_full() {
                return *self;
            }
            // Check if this interval will be empty after contraction.
            if self.length() + 2.0 * margin - 2.0 * f64::EPSILON <= 0.0 {
                return S1Interval::empty();
            }
        }
        let mut new_lo = remainder(self.lo - margin, 2.0 * PI);
        if new_lo <= -PI {
            new_lo = PI;
        }
        let new_hi = remainder(self.hi + margin, 2.0 * PI);
        S1Interval::new_checked(new_lo, new_hi)
    }

    /// Returns the smallest interval containing both this and the given interval.
    pub fn union(&self, y: &S1Interval) -> S1Interval {
        if y.is_empty() {
            return *self;
        }
        if self.fast_contains(y.lo) {
            if self.fast_contains(y.hi) {
                // Either this interval contains y, or the union is Full.
                if self.contains(y) {
                    return *self;
                }
                return S1Interval::full();
            }
            return S1Interval::new_checked(self.lo, y.hi);
        }
        if self.fast_contains(y.hi) {
            return S1Interval::new_checked(y.lo, self.hi);
        }

        // This interval contains neither endpoint of y. Either y contains all of this interval, or
        // the two intervals are disjoint.
        if self.is_empty() || y.fast_contains(self.lo) {
            return *y;
        }

        // Check which pair of endpoints are closer together.
        let dlo = positive_distance(y.hi, self.lo);
        let dhi = positive_distance(self.hi, y.lo);
        if dlo < dhi {
            S1Interval::new_checked(y.lo, self.hi)
        } else {
            S1Interval::new_checked(self.lo, y.hi)
        }
    }

    /// Returns the intersection of this interval with the given interval. Note that the region of
    /// intersection may consist of two disjoint subintervals. In that case, this returns the
    /// shorter of the two original intervals.
    pub fn intersection(&self, y: &S1Interval) -> S1Interval {
        if y.is_empty() {
            return S1Interval::empty();
        }
        if self.fast_contains(y.lo) {
            if self.fast_contains(y.hi) {
                // Either this interval contains y, or the intersection consists of two disjoint
                // subintervals. Return the shorter original.
                if y.length() < self.length() {
                    return *y;
                }
                return *self;
            }
            return S1Interval::new_checked(y.lo, self.hi);
        }
        if self.fast_contains(y.hi) {
            return S1Interval::new_checked(self.lo, y.hi);
        }

        // This interval contains neither endpoint of y. Either y contains all of this interval, or
        // the two intervals are disjoint.
        if y.fast_contains(self.lo) {
            return *self;
        }
        debug_assert!(!self.intersects(y));
        S1Interval::empty()
    }

    /// Return true if this interval can be transformed into the given interval by moving each
    /// endpoint by at most "max_error" (and without the endpoints crossing, which would invert the
    /// interval).  Empty and full intervals are considered to start at an arbitrary point on the
    /// unit circle, thus any interval with (length <= 2*max_error) matches the empty interval, and
    /// any interval with (length >= 2*Pi - 2*max_error) matches the full interval.
    pub fn approx_equals(&self, y: &S1Interval, max_error: f64) -> bool {
        if self.is_empty() {
            return y.length() <= 2.0 * max_error;
        }
        if y.is_empty() {
            return self.length() <= 2.0 * max_error;
        }
        if self.is_full() {
            return y.length() >= 2.0 * (PI - max_error);
        }
        if y.is_full() {
            return self.length() >= 2.0 * (PI - max_error);
        }

        // Check that moving endpoints does not invert the interval.
        remainder(y.lo - self.lo, 2.0 * PI).abs() <= max_error
            && remainder(y.hi - self.hi, 2.0 * PI).abs() <= max_error
            && (self.length() - y.length()).abs() <= 2.0 * max_error
    }
}

impl PartialEq for S1Interval {
    fn eq(&self, other: &Self) -> bool {
        self.lo == other.lo && self.hi == other.hi
    }
}

impl std::fmt::Display for S1Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.lo, self.hi)
    }
}

/// Computes the distance from `a` to `b` in the range [0, 2*PI). This is more numerically stable
/// than `remainder(b - a - PI, 2*PI) + PI` for very small positive distances.
fn positive_distance(a: f64, b: f64) -> f64 {
    let d = b - a;
    if d >= 0.0 {
        d
    } else {
        // Ensure that if b == PI and a == (-PI + eps), we return ~2*PI.
        (b + PI) - (a - PI)
    }
}

#[cfg(test)]
#[path = "tests/s1interval_tests.rs"]
mod tests;
