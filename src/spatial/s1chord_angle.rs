//! S1ChordAngle represents an angle as the squared chord length between two points on the unit
//! sphere. Chord angles are efficient for computing and comparing distances without trigonometry.
//!
//! Ported from s2/s1chord_angle.h (Eric Veach, Google).

use std::f64::consts::PI;

/// Maximum relative error when summing two S1ChordAngles together.  Absolute error is length2() of
/// the summed S1ChordAngle times this value.  See the definition of operator+() for the error
/// analysis.
pub const RELATIVE_SUM_ERROR: f64 = 2.02 * f64::EPSILON;

const MAX_LENGTH2: f64 = 4.0;

/// S1ChordAngle represents the angle subtended by a chord (i.e., the straight line segment
/// connecting two points on the unit sphere).  Its representation makes it very efficient for
/// computing and comparing distances, but unlike S1Angle it is only capable of representing angles
/// between 0 and Pi radians.
#[derive(Clone, Copy, Debug)]
pub struct S1ChordAngle {
    length2: f64,
}

impl S1ChordAngle {
    /// Return the zero chord angle.
    pub const fn zero() -> Self {
        Self { length2: 0.0 }
    }

    /// Return a chord angle of 90 degrees (a "right angle").
    pub const fn right() -> Self {
        Self { length2: 2.0 }
    }

    /// Return a chord angle of 180 degrees (a "straight angle").  This is the maximum finite
    /// chord angle.
    pub const fn straight() -> Self {
        Self { length2: 4.0 }
    }

    /// Return a chord angle larger than any finite chord angle.  The only valid operations on
    /// Infinity() are comparisons, conversions, and Successor() / Predecessor().
    pub const fn infinity() -> Self {
        Self {
            length2: f64::INFINITY,
        }
    }

    /// Return a chord angle smaller than Zero().  The only valid operations on Negative() are
    /// comparisons, conversions, and Successor() / Predecessor().
    pub const fn negative() -> Self {
        Self { length2: -1.0 }
    }

    /// Construct the S1ChordAngle corresponding to the distance between the two given points. The
    /// points must be unit length.
    pub fn from_points(x: &[f64; 3], y: &[f64; 3]) -> Self {
        let dx = x[0] - y[0];
        let dy = x[1] - y[1];
        let dz = x[2] - y[2];
        let length2 = dx * dx + dy * dy + dz * dz;
        // The squared distance may slightly exceed 4.0 due to roundoff errors.
        Self {
            length2: length2.min(MAX_LENGTH2),
        }
    }

    /// Construct an S1ChordAngle from the squared chord length.  Note that the argument is
    /// automatically clamped to a maximum of 4.0 to handle possible roundoff errors.  The argument
    /// must be non-negative.
    pub fn from_length2(length2: f64) -> Self {
        debug_assert!(length2 >= 0.0);
        Self {
            length2: length2.min(MAX_LENGTH2),
        }
    }

    /// Conversion from an angle in radians.  Angles outside the range [0, Pi] are handled as
    /// follows: Infinity is mapped to Infinity(), negative angles are mapped to Negative(), and
    /// finite angles larger than Pi are mapped to Straight().
    pub fn from_radians(radians: f64) -> Self {
        if radians < 0.0 {
            return Self::negative();
        }
        if !radians.is_finite() {
            return Self::infinity();
        }
        // The chord length is 2 * sin(angle / 2).
        let length = 2.0 * (0.5 * radians.min(PI)).sin();
        Self {
            length2: length * length,
        }
    }

    /// Convenience method implemented by converting from radians.
    pub fn from_degrees(degrees: f64) -> Self {
        Self::from_radians(degrees.to_radians())
    }

    /// This method uses the distance along the surface of the sphere as an upper bound on the
    /// distance through the sphere's interior.  Accurate to within 1% for distances up to about
    /// 3100km on the Earth's surface.
    pub fn fast_upper_bound_from_radians(radians: f64) -> Self {
        Self::from_length2(radians * radians)
    }

    /// The squared length of the chord.  (Most clients will not need this.)
    #[inline]
    pub fn length2(&self) -> f64 {
        self.length2
    }

    /// Converts to radians.  Note that the conversion uses trigonometric functions and therefore
    /// should be avoided in inner loops.
    pub fn to_radians(&self) -> f64 {
        if self.is_negative() {
            return -1.0;
        }
        if self.is_infinity() {
            return f64::INFINITY;
        }
        2.0 * (0.5 * self.length2.sqrt()).asin()
    }

    /// Convenience method implemented by calling to_radians() first.
    pub fn to_degrees(&self) -> f64 {
        self.to_radians().to_degrees()
    }

    /// Returns true if this is the zero angle.
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.length2 == 0.0
    }

    /// Returns true if this is negative.
    #[inline]
    pub fn is_negative(&self) -> bool {
        self.length2 < 0.0
    }

    /// Returns true if this is infinite.
    #[inline]
    pub fn is_infinity(&self) -> bool {
        self.length2 == f64::INFINITY
    }

    /// Negative or infinity.
    #[inline]
    pub fn is_special(&self) -> bool {
        self.is_negative() || self.is_infinity()
    }

    /// Return true if the internal representation is valid.  Negative() and Infinity() are both
    /// considered valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        (self.length2 >= 0.0 && self.length2 <= MAX_LENGTH2) || self.is_special()
    }

    /// Returns sin(a)**2, but computed more efficiently.
    pub fn sin2(&self) -> f64 {
        debug_assert!(!self.is_special());
        // Let "a" be the (non-squared) chord length, and let A be the
        // corresponding half-angle (a = 2*sin(A)).  The formula below can be
        // derived from:
        //   sin(2*A) = 2 * sin(A) * cos(A)
        //   cos^2(A) = 1 - sin^2(A)
        self.length2 * (1.0 - 0.25 * self.length2)
    }

    /// Returns sin of the angle.
    pub fn sin(&self) -> f64 {
        self.sin2().sqrt()
    }

    /// Returns cos of the angle.
    pub fn cos(&self) -> f64 {
        debug_assert!(!self.is_special());
        // cos(2*A) = cos^2(A) - sin^2(A) = 1 - 2*sin^2(A)
        1.0 - 0.5 * self.length2
    }

    /// Returns tan of the angle.
    pub fn tan(&self) -> f64 {
        self.sin() / self.cos()
    }

    /// Returns the smallest representable S1ChordAngle larger than this object.  This can be used
    /// to convert a "<" comparison to a "<=" comparison.
    ///
    /// Note the following special cases:
    ///   Negative().Successor() == Zero()
    ///   Straight().Successor() == Infinity()
    ///   Infinity().Successor() == Infinity()
    pub fn successor(&self) -> Self {
        if self.length2 >= MAX_LENGTH2 {
            return Self::infinity();
        }
        if self.length2 < 0.0 {
            return Self::zero();
        }
        Self {
            length2: next_after(self.length2, 10.0),
        }
    }

    /// Like Successor(), but returns the largest representable S1ChordAngle less than this object.
    ///
    /// Note the following special cases:
    ///   Infinity().Predecessor() == Straight()
    ///   Zero().Predecessor() == Negative()
    ///   Negative().Predecessor() == Negative()
    pub fn predecessor(&self) -> Self {
        if self.length2 <= 0.0 {
            return Self::negative();
        }
        if self.length2 > MAX_LENGTH2 {
            return Self::straight();
        }
        Self {
            length2: next_after(self.length2, -10.0),
        }
    }

    /// Returns a new S1ChordAngle that has been adjusted by the given error bound (which can be
    /// positive or negative).  "error" should be the value returned by one of the error bound
    /// methods below.  If the angle is Negative() or Infinity(), don't change it. Otherwise clamp
    /// it to the valid range.
    pub fn plus_error(&self, error: f64) -> Self {
        if self.is_special() {
            return *self;
        }
        Self {
            length2: (self.length2 + error).clamp(0.0, MAX_LENGTH2),
        }
    }

    /// Return the maximum error in length2() for the S1ChordAngle(x, y) constructor, assuming that
    /// "x" and "y" are normalized to within the bounds guaranteed by S2Point::Normalize().  (The
    /// error is defined with respect to the true distance after the points are projected to lie
    /// exactly on the sphere.)
    pub fn get_s2point_constructor_max_error(&self) -> f64 {
        // There is a relative error of 2.5 * DBL_EPSILON when computing the
        // squared distance, plus a relative error of 2 * DBL_EPSILON and an
        // absolute error of (16 * DBL_EPSILON**2) because the lengths of the
        // input points may differ from 1 by up to (2 * DBL_EPSILON) each.
        4.5 * f64::EPSILON * self.length2 + 16.0 * f64::EPSILON * f64::EPSILON
    }
}

impl PartialEq for S1ChordAngle {
    fn eq(&self, other: &Self) -> bool {
        self.length2 == other.length2
    }
}

impl Eq for S1ChordAngle {}

impl PartialOrd for S1ChordAngle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.length2.partial_cmp(&other.length2)
    }
}

impl Ord for S1ChordAngle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.length2.partial_cmp(&other.length2).unwrap()
    }
}

impl std::ops::Add for S1ChordAngle {
    type Output = Self;

    // Note that this method is much more efficient than converting the chord angles to S1Angles
    // and adding those.  It requires only one square root plus a few additions and
    // multiplications.
    fn add(self, other: Self) -> Self {
        debug_assert!(!self.is_special());
        debug_assert!(!other.is_special());

        let a2 = self.length2;
        let b2 = other.length2;

        // Optimization for the common case where "b" is an error tolerance parameter that happens
        // to be set to zero.
        if b2 == 0.0 {
            return self;
        }

        // Clamp the angle sum to at most 180 degrees.
        if a2 + b2 >= MAX_LENGTH2 {
            return Self::straight();
        }

        // Let "a" and "b" be the (non-squared) chord lengths, and let c = a+b.
        // Let A, B, and C be the corresponding half-angles (a = 2*sin(A), etc).
        // Then the formula below can be derived from c = 2 * sin(A+B) and the
        // relationships   sin(A+B) = sin(A)*cos(B) + sin(B)*cos(A)
        //                 cos(X) = sqrt(1 - sin^2(X)) .
        let x = a2 * (1.0 - 0.25 * b2); // is_valid() => non-negative
        let y = b2 * (1.0 - 0.25 * a2); // is_valid() => non-negative
        Self {
            length2: (x + y + 2.0 * (x * y).sqrt()).min(MAX_LENGTH2),
        }
    }
}

impl std::ops::Sub for S1ChordAngle {
    type Output = Self;

    // See comments in operator+().
    fn sub(self, other: Self) -> Self {
        debug_assert!(!self.is_special());
        debug_assert!(!other.is_special());

        let a2 = self.length2;
        let b2 = other.length2;

        if b2 == 0.0 {
            return self;
        }
        if a2 <= b2 {
            return Self::zero();
        }

        let x = a2 * (1.0 - 0.25 * b2);
        let y = b2 * (1.0 - 0.25 * a2);

        // The calculation below is formulated differently (with two square roots rather than one)
        // to avoid excessive cancellation error when two nearly equal values are subtracted.
        let c = (x.sqrt() - y.sqrt()).max(0.0);
        Self { length2: c * c }
    }
}

impl std::ops::AddAssign for S1ChordAngle {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
    }
}

impl std::ops::SubAssign for S1ChordAngle {
    fn sub_assign(&mut self, other: Self) {
        *self = *self - other;
    }
}

impl std::fmt::Display for S1ChordAngle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}deg", self.to_degrees())
    }
}

fn next_after(x: f64, y: f64) -> f64 {
    if x == y {
        return y;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        return if y > 0.0 {
            f64::from_bits(1)
        } else {
            f64::from_bits(1u64 << 63 | 1)
        };
    }
    let bits = x.to_bits();
    let next_bits = if (x < y) == (x > 0.0) {
        bits + 1
    } else {
        bits - 1
    };
    f64::from_bits(next_bits)
}

#[cfg(test)]
#[path = "tests/s1chord_angle_tests.rs"]
mod tests;
