//! Edge crossing predicates and the S2EdgeCrosser for efficient edge intersection testing.
//!
//! The predicates in this module are robust, meaning they produce correct, consistent results even
//! in pathological cases.
//!
//! From s2predicates.h, s2predicates.cc, s2edge_crossings.h and s2edge_crossings.cc.
use num_traits::Zero;

use super::exact::{exact_cross, expensive_sign, to_exact, ExactPoint};
use super::math::{cross, dot, ldexp, norm2};

/// Error bound for tangent plane rejection test in edge crossing.
///
/// The maximum error in computing tangent planes for fast rejection is:
/// - Error in RobustCrossProd() is insignificant
/// - Error in CrossProd() call: (0.5 + 1/sqrt(3)) * DBL_EPSILON
/// - Error in each DotProd() call: DBL_EPSILON
/// - Total: (1.5 + 1/sqrt(3)) * DBL_EPSILON
///
/// CrossingSignInternal2 in s2edge_crosser.cc
const TANGENT_ERROR: f64 = (1.5 + 1.0 / 1.732050807568877_f64) * f64::EPSILON;

/// sqrt(3) constant for error bound computations.
///
/// Google S2 value 1.7320508075688772935274463415058 is truncated to fit f64.
///
/// kSqrt3 in s2predicates_internal.cc
const SQRT3: f64 = 1.732_050_807_568_877_2_f64;

/// Rounding error for f64 arithmetic: 0.5 * f64::EPSILON = 2^(-53) using the simplifed method
/// mentioned in s2predicates_internal.h.
///
/// DBL_ERR in s2predicates_internal.h
const DBL_ERR: f64 = 0.5 * f64::EPSILON;

/// Upper bound on the angle (in radians) between the vector returned by robust_cross(a, b) and the
/// true cross product of a and b.
///
/// This value can be set somewhat arbitrarily because the algorithm uses more precision as needed
/// to achieve the specified error. The only strict requirement is kRobustCrossProdError >= DBL_ERR
/// (since this is the minimum error even when using exact arithmetic). We set the error to
/// 6 * DBL_ERR (= 3 * DBL_EPSILON) so that virtually all cases can be handled using ordinary
/// double-precision arithmetic.
///
/// kRobustCrossProdError in s2edge_crossings.h
/// static_assert on kRobustCrossProdError in s2edge_crossings.cc
pub const ROBUST_CROSS_PROD_ERROR: f64 = 6.0 * DBL_ERR;

/// Minimum norm threshold for the stable cross product computation.
///
/// The directional error in computing (a-b) x (a+b) for unit vectors a, b is:
///
/// ```text
/// (1 + 2*sqrt(3) + 32*sqrt(3)*DBL_ERR/||N||) * T_ERR
/// ````
///
/// where ||N|| is the norm of the result and T_ERR is the rounding error for the precision
/// type T (for f64, T_ERR = DBL_ERR = 0.5 * EPSILON).
///
/// To keep this error to at most ROBUST_CROSS_PROD_ERROR, we require:
///
/// ```text
/// ||N|| >= 32*sqrt(3)*DBL_ERR / (ROBUST_CROSS_PROD_ERROR/T_ERR - (1 + 2*sqrt(3)))
/// ````
///
/// For double precision where T_ERR = DBL_ERR:
///
/// ```text
/// kMinNorm = (32 * sqrt(3) * DBL_ERR) / (6.0 - (1 + 2*sqrt(3)))
///          = (32 * sqrt(3) * DBL_ERR) / (5 - 2*sqrt(3))
///          ~= 4.00644e-15 (~= 18.04 * f64::EPSILON)
/// ````
///
/// GetStableCrossProd<T> in s2edge_crossings.cc
pub const ROBUST_CROSS_PROD_MIN_NORM: f64 =
    (32.0 * SQRT3 * DBL_ERR) / (ROBUST_CROSS_PROD_ERROR / DBL_ERR - (1.0 + 2.0 * SQRT3));

const MAX_DET_ERROR: f64 = 3.6548 * f64::EPSILON;

/// This version of Sign returns +1 if the points are definitely CCW, -1 if they are definitely CW,
/// and 0 if two points are identical or the result is uncertain.  Uncertain cases can be resolved,
/// if desired, by calling ExpensiveSign.
///
/// The purpose of this method is to allow additional cheap tests to be done, where possible, in
/// order to avoid calling ExpensiveSign unnecessarily.
///
/// REQUIRES: a_cross_b == cross(a, b)
pub fn triage_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], a_cross_b: &[f64; 3]) -> i32 {
    assert!(norm2(a) <= 2.0 + f64::EPSILON);
    assert!(norm2(b) <= 2.0 + f64::EPSILON);
    assert!(norm2(c) <= 2.0 + f64::EPSILON);

    let det = dot(a_cross_b, c);

    if det > MAX_DET_ERROR {
        1
    } else if det < -MAX_DET_ERROR {
        -1
    } else {
        0
    }
}

/// Returns +1 if the points A, B, C are counterclockwise, -1 if the points are clockwise, and 0 if
/// any two points are the same.  This function is essentially like taking the sign of the
/// determinant of ABC, except that it has additional logic to make sure that the above properties
/// hold even when the three points are coplanar, and to deal with the limitations of
/// floating-point arithmetic.
///
/// Sign satisfies the following conditions:
///
///  (1) Sign(a,b,c) == 0 if and only if a == b, b == c, or c == a
///  (2) Sign(b,c,a) == Sign(a,b,c) for all a,b,c
///  (3) Sign(c,b,a) == -Sign(a,b,c) for all a,b,c
///
/// In other words:
///
///  (1) The result is zero if and only if two points are the same.
///  (2) Rotating the order of the arguments does not affect the result.
///  (3) Exchanging any two arguments inverts the result.
///
/// On the other hand, note that it is not true in general that Sign(-a,b,c) == -Sign(a,b,c), or
/// any similar identities involving antipodal points.
pub fn sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> i32 {
    // We don't need RobustCrossProd() here because Sign() does its own error estimation and calls
    // ExpensiveSign() if there is any uncertainty about the result.
    sign_with_cross(a, b, c, &cross(a, b))
}

/// Like sign(), but allows the precomputed cross-product of A and B to be specified.
pub fn sign_with_cross(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], a_cross_b: &[f64; 3]) -> i32 {
    let result = triage_sign(a, b, c, a_cross_b);
    if result != 0 {
        return result;
    }
    expensive_sign(a, b, c, true)
}

/// Given 4 points on the unit sphere, return true if the edges OA, OB, and OC are encountered in
/// that order while sweeping CCW around the point O. You can think of this as testing whether A <=
/// B <= C with respect to the CCW ordering around O that starts at A, or equivalently, whether B
/// is contained in the range of angles (inclusive) that starts at A and extends
/// CCW to C.
///
/// Properties:
///
///  (1) If OrderedCCW(a,b,c,o) && OrderedCCW(b,a,c,o), then a == b
///  (2) If OrderedCCW(a,b,c,o) && OrderedCCW(a,c,b,o), then b == c
///  (3) If OrderedCCW(a,b,c,o) && OrderedCCW(c,b,a,o), then a == b == c
///  (4) If a == b or b == c, then OrderedCCW(a,b,c,o) is true
///  (5) Otherwise if a == c, then OrderedCCW(a,b,c,o) is false
///
/// REQUIRES: a != o && b != o && c != o
pub fn ordered_ccw(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], o: &[f64; 3]) -> bool {
    assert!(a != o);
    assert!(b != o);
    assert!(c != o);

    // Count how many of the three "is_left_of" tests pass.
    // The last test uses > instead of >= so that:
    // - We return true if A == B or B == C
    // - We return false if A == C (and A != B)
    let mut sum = 0;
    if sign(b, o, a) >= 0 {
        sum += 1;
    }
    if sign(c, o, b) >= 0 {
        sum += 1;
    }
    if sign(a, o, c) > 0 {
        sum += 1;
    }
    sum >= 2
}

/// Returns a vector whose direction is guaranteed to be very close to the exact mathematical cross
/// product of the given unit-length vectors "a" and "b", but whose magnitude is arbitrary.  Unlike
/// a.CrossProd(b), this statement is true even when "a" and "b" are very nearly
/// parallel (i.e., a ~= b or a ~= -b). Specifically, the direction of the result vector differs
/// from the exact cross product by at most kRobustCrossProdError radians (see below).
///
/// When a == -b exactly, the result is consistent with the symbolic perturbation model used by
/// S2::Sign (see s2predicates.h).  In other words, even antipodal point pairs have a consistent
/// and well-defined edge between them.  (In fact this is true for any pair of distinct points
/// whose vectors are parallel.)
///
/// When a == b exactly, an arbitrary vector orthogonal to "a" is returned. [From a strict
/// mathematical viewpoint it would be better to return (0, 0, 0), but this behavior helps to avoid
/// special cases in client code.]
///
/// This function has the following properties (RCP == RobustCrossProd):
///
/// (1) RCP(a, b) != 0 for all a, b
/// (2) RCP(b, a) == -RCP(a, b) unless a == b
/// (3) RCP(-a, b) == -RCP(a, b) unless a and b are exactly proportional
/// (4) RCP(a, -b) == -RCP(a, b) unless a and b are exactly proportional
///
/// Note that if you want the result to be unit-length, you must call Normalize() explicitly.  (The
/// result is always scaled such that Normalize() can be called without precision loss due to
/// floating-point underflow.)
pub fn robust_cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    assert!((norm2(a) - 1.0).abs() < 5.0 * f64::EPSILON);
    assert!((norm2(b) - 1.0).abs() < 5.0 * f64::EPSILON);

    // (a - b) x (a + b) = 2 * (a x b), more stable when nearly parallel
    let diff = [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
    let sum = [a[0] + b[0], a[1] + b[1], a[2] + b[2]];

    let result = cross(&diff, &sum);

    let norm2 = norm2(&result);
    if norm2 >= ROBUST_CROSS_PROD_MIN_NORM * ROBUST_CROSS_PROD_MIN_NORM {
        return result;
    }

    if a == b {
        return ortho(a);
    }

    exact_cross_prod(a, b)
}

// Computes the cross product of two unit-length vectors using exact arithmetic. If the exact
// result is non-zero, converts it to a normalizable f64 vector. Otherwise falls back to symbolic
// perturbation.
fn exact_cross_prod(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    assert!(a != b);
    let xa = to_exact(a);
    let xb = to_exact(b);
    let result = exact_cross(&xa, &xb);
    if !result[0].is_zero() || !result[1].is_zero() || !result[2].is_zero() {
        normalizable_from_exact(&result)
    } else {
        symbolic_cross_prod(a, b)
    }
}

// Converts a BigRational vector to f64, scaling as necessary to ensure the result can be
// normalized without loss of precision due to underflow.  When the exact components are too
// small for direct f64 conversion, we scale in the exact domain first -- dividing by the
// largest absolute component so the largest becomes +/-1.0, then converting.  The division
// is exact in BigRational; only the final to_f64() rounds.
fn normalizable_from_exact(xf: &ExactPoint) -> [f64; 3] {
    use num_traits::{Signed, ToPrimitive};

    // Try direct conversion.
    let x = [
        xf[0].to_f64().unwrap_or(0.0),
        xf[1].to_f64().unwrap_or(0.0),
        xf[2].to_f64().unwrap_or(0.0),
    ];
    if is_normalizable(&x) {
        return x;
    }

    // Direct conversion underflowed.  Scale in the exact domain by dividing
    // by the largest absolute component, then convert.
    let mut abs_max = xf[0].abs();
    for i in 1..3 {
        let a = xf[i].abs();
        if a > abs_max {
            abs_max = a;
        }
    }

    if abs_max.is_zero() {
        return [0.0, 0.0, 0.0];
    }

    [
        (&xf[0] / &abs_max)
            .to_f64()
            .expect("scaled component in [-1, 1]"),
        (&xf[1] / &abs_max)
            .to_f64()
            .expect("scaled component in [-1, 1]"),
        (&xf[2] / &abs_max)
            .to_f64()
            .expect("scaled component in [-1, 1]"),
    ]
}

// In S2, ortho is a general-purpose geometric utility that returns any orthogonal vector, while
// ref_dir is a semantic construct for the semi-open boundary model used in vertex containment
// queries. They share an implementation, but code calling ref_dir is participating in the
// containment protocol, while code calling ortho is just doing geometry. S2 separated them for
// clarity of intent, and the one-line wrapper is intentional.

/// Returns a unit-length vector that is orthogonal to "a".  Satisfies Ortho(-a) = -Ortho(a) for
/// all a.
///
/// Note that Vector3_d also defines an "Ortho" method, but this one is preferred for use in S2
/// code because it explicitly tries to avoid result coordinates that are zero.  (This is a
/// performance optimization that reduces the amount of time spent in functions that handle
/// degeneracies.)
///
/// S2::Ortho in s2pointutil.h and s2pointutil.cc
fn ortho(a: &[f64; 3]) -> [f64; 3] {
    let ax = a[0].abs();
    let ay = a[1].abs();
    let az = a[2].abs();

    // Strict > mirrors LargestAbsComponent() in vector.h.
    let largest = if ax > ay {
        if ax > az {
            0
        } else {
            2
        }
    } else if ay > az {
        1
    } else {
        2
    };

    // s2pointutil.cc: k = LargestAbsComponent() - 1, wrapping to 2.
    let k = if largest == 0 { 2 } else { largest - 1 };
    let mut temp = [0.012, 0.0053, 0.00457];
    temp[k] = 1.0;

    let cross = cross(a, &temp);

    // Normalize
    let norm = norm2(&cross).sqrt();
    [cross[0] / norm, cross[1] / norm, cross[2] / norm]
}

/// Returns a unit-length vector used as the reference direction for deciding whether a polygon
/// with semi-open boundaries contains the given vertex "a" (see S2ContainsVertexQuery).  The
/// result is unit length and is guaranteed to be different from the given point "a".
///
/// S2::RefDir in s2pointutil.h
#[inline]
pub fn ref_dir(a: &[f64; 3]) -> [f64; 3] {
    ortho(a)
}

#[inline]
fn ilogb(x: f64) -> i32 {
    let bits = x.to_bits();
    let exp = ((bits >> 52) & 0x7FF) as i32;
    if exp == 0 {
        // Subnormal: true exponent depends on leading zeros in mantissa.
        let mantissa = bits & 0x000F_FFFF_FFFF_FFFF;
        -1011 - mantissa.leading_zeros() as i32
    } else {
        exp - 1023
    }
}

/// Returns true if the given vector's magnitude is large enough such that the angle to another
/// vector of the same magnitude can be measured using Angle() without loss of precision due to
/// floating-point underflow.  (This requirement is also sufficient to ensure that Normalize() can
/// be called without risk of precision loss.)
///
/// Let ab = RobustCrossProd(a, b) and cd = RobustCrossProd(cd).  In order for ab.Angle(cd) to not
/// lose precision, the squared magnitudes of ab and cd must each be at least 2**-484.  This
/// ensures that the sum of the squared magnitudes of ab.CrossProd(cd) and ab.DotProd(cd) is at
/// least 2**-968, which ensures that any denormalized terms in these two calculations do not
/// affect the accuracy of the result (since all denormalized numbers are smaller than 2**-1022,
/// which is less than DBL_ERR * 2**-968).
///
/// The fastest way to ensure this is to test whether the largest component of the result has a
/// magnitude of at least 2**-242.
///
/// IsNormalizable in s2edge_crossings.cc
#[inline]
fn is_normalizable(p: &[f64; 3]) -> bool {
    const MIN_COMPONENT: f64 = 1.0_f64
        / (1_u64 << 62) as f64
        / (1_u64 << 62) as f64
        / (1_u64 << 62) as f64
        / (1_u64 << 56) as f64; // 2^(-242)
    p[0].abs().max(p[1].abs().max(p[2].abs())) >= MIN_COMPONENT
}

/// Scales a 3-vector as necessary to ensure that the result can be normalized
/// without loss of precision due to floating-point underflow.
///
/// REQUIRES: p != (0, 0, 0)
///
/// We can't just scale by a fixed factor because the smallest representable double is 2**-1074, so
/// if we multiplied by 2**(1074 - 242) then the result might be so large that we couldn't square
/// it without overflow.
///
/// Note that we must scale by a power of two to avoid rounding errors, and that the calculation of
/// "pmax" is free because IsNormalizable() is inline.  The code below scales "p" such that the
/// largest component is
/// in the range [1, 2).
///
/// EnsureNormalizable in s2edge_crossings.cc
#[inline]
fn ensure_normalizable(p: [f64; 3]) -> [f64; 3] {
    assert!(p != [0.0, 0.0, 0.0]);
    if !is_normalizable(&p) {
        let p_max = p[0].abs().max(p[1].abs().max(p[2].abs()));
        // The expression below avoids signed overflow for any value of ilogb().
        // ldexp(2, -1 - ilogb(p_max)) scales p so largest component is in [1, 2).
        let scale = ldexp(2.0, -1 - ilogb(p_max));
        [p[0] * scale, p[1] * scale, p[2] * scale]
    } else {
        p
    }
}

/// Computes the cross product of a and b using symbolic perturbations.
///
/// REQUIRES: a < b (lexicographic ordering)
/// REQUIRES: The exact cross product a x b is zero
///
/// The following code uses the same symbolic perturbation model as S2::Sign. The particular
/// sequence of tests below was obtained using Mathematica (although it would be easy to do it by
/// hand for this simple case).
///
/// Just like the function SymbolicallyPerturbedSign() in s2predicates.cc, every input coordinate
/// x[i] is assigned a symbolic perturbation dx[i].  We then compute the cross product
///
/// ```text
/// (a + da).CrossProd(b + db) .
/// ```
///
/// The result is a polynomial in the perturbation symbols.  For example if we did this in one
/// dimension, the result would be
///
/// ```text
/// a * b + b * da + a * db + da * db
/// ```
///
/// where "a" and "b" have numerical values and "da" and "db" are symbols. In 3 dimensions the
/// result is similar except that the coefficients are 3-vectors rather than scalars.
///
/// Every possible S2Point has its own symbolic perturbation in each coordinate (i.e., there are
/// about 3 * 2**192 symbols).  The magnitudes of the perturbations are chosen such that if x < y
/// lexicographically, the perturbations for "y" are much smaller than the perturbations for "x".
/// Similarly, the perturbations for the coordinates of a given point x are chosen such that dx[0]
/// is much smaller than dx[1] which is much smaller than dx[2].  Putting this together with fact
/// the inputs to this function have been sorted so that a < b lexicographically, this tells us
/// that
///
/// ```text
/// da[2] > da[1] > da[0] > db[2] > db[1] > db[0]
/// ```
///
/// where each perturbation is so much smaller than the previous one that we don't even need to
/// consider it unless the coefficients of all previous perturbations are zero.  In fact, each
/// succeeding perturbation is so small that we don't need to consider it unless the coefficient of
/// all products of the previous perturbations are zero.  For example, we don't need to consider
/// the coefficient of db[1] unless the coefficient of db[2]*da[0] is zero.
///
/// The follow code simply enumerates the coefficients of the perturbations (and products of
/// perturbations) that appear in the cross product above, in order of decreasing perturbation
/// magnitude.  The first non-zero coefficient determines the result.  The easiest way to enumerate
/// the coefficients in the correct order is to pretend that each perturbation is some tiny value
/// "eps" raised to a power of two:
///
/// ```text
/// eps**    1      2      4      8     16     32
///        da[2]  da[1]  da[0]  db[2]  db[1]  db[0]
/// ```
///
/// Essentially we can then just count in binary and test the corresponding subset of perturbations
/// at each step.  So for example, we must test the coefficient of db[2]*da[0] before db[1] because
/// eps**12 > eps**16.
///
/// SymbolicCrossProdSorted in s2edge_crossings.cc
fn symbolic_cross_prod_sorted(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    assert!(a < b);
    assert!({
        let xa = to_exact(a);
        let xb = to_exact(b);
        let c = exact_cross(&xa, &xb);
        c[0].is_zero() && c[1].is_zero() && c[2].is_zero()
    });

    if b[0] != 0.0 || b[1] != 0.0 {
        // da[2]
        return [-b[1], b[0], 0.0];
    }
    if b[2] != 0.0 {
        // da[1]
        return [b[2], 0.0, 0.0]; // Note that b[0] == 0.
    }

    // None of the remaining cases can occur in practice, because we can only get
    // to this point if b = (0, 0, 0).  Nevertheless, even (0, 0, 0) has a
    // well-defined direction under the symbolic perturbation model.
    assert!(b[1] == 0.0 && b[2] == 0.0); // da[0] coefficients (always zero)

    if a[0] != 0.0 || a[1] != 0.0 {
        // db[2]
        return [a[1], -a[0], 0.0];
    }

    // The following coefficient is always non-zero, so we can stop here.
    //
    // It may seem strange that we are returning (1, 0, 0) as the cross product
    // without even looking at the sign of a[2].  (Wouldn't you expect
    // (0, 0, -1) x (0, 0, 0) and (0, 0, 1) x (0, 0, 0) to point in opposite
    // directions?)  It's worth pointing out that in this function there is *no
    // relationship whatsoever* between the vectors "a" and "-a", because the
    // perturbations applied to these vectors may be entirely different.  This is
    // why the identity "RobustCrossProd(-a, b) == -RobustCrossProd(a, b)" does
    // not hold whenever "a" and "b" are linearly dependent (i.e., proportional).
    // [As it happens the two cross products above actually do point in opposite
    // directions, but for example (1, 1, 1) x (2, 2, 2) = (-2, 2, 0) and
    // (-1, -1, -1) x (2, 2, 2) = (-2, 2, 0) do not.]
    [1.0, 0.0, 0.0] // db[2] * da[1]
}

/// Computes the cross product of a and b using symbolic perturbations.
///
/// REQUIRES: a != b
///
/// SymbolicCrossProdSorted() requires that a < b.
///
/// SymbolicCrossProd in s2edge_crossings.cc
fn symbolic_cross_prod(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    assert!(a != b);
    // SymbolicCrossProdSorted() requires that a < b.
    if a < b {
        ensure_normalizable(symbolic_cross_prod_sorted(a, b))
    } else {
        let result = ensure_normalizable(symbolic_cross_prod_sorted(b, a));
        [-result[0], -result[1], -result[2]]
    }
}

/// Compute the determinant in a numerically stable way.  Unlike triage_sign(), this method can
/// usually compute the correct determinant sign even when all three points are as collinear as
/// possible.  For example if three points are spaced 1km apart along a random line on the Earth's
/// surface using the nearest representable points, there is only a 0.4% chance that this method
/// will not be able to find the determinant sign.  The probability of failure decreases as the
/// points get closer together; if the collinear points are
/// 1 meter apart, the failure rate drops to 0.0004%.
///
/// This method could be extended to also handle nearly-antipodal points (and in fact an earlier
/// version of this code did exactly that), but antipodal points are rare in practice so it seems
/// better to simply fall back to exact arithmetic in that case.
///
/// StableSign in s2predicates.cc
pub fn stable_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> i32 {
    let ab = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    let bc = [c[0] - b[0], c[1] - b[1], c[2] - b[2]];
    let ca = [a[0] - c[0], a[1] - c[1], a[2] - c[2]];
    let ab2 = norm2(&ab);
    let bc2 = norm2(&bc);
    let ca2 = norm2(&ca);

    // Now compute the determinant ((A-C)x(B-C)).C, where the vertices have been
    // cyclically permuted if necessary so that AB is the longest edge.  (This
    // minimizes the magnitude of cross product.)  At the same time we also
    // compute the maximum error in the determinant.  Using a similar technique
    // to the one used for kMaxDetError, the error is at most
    //
    //   |d| <= (3 + 6/sqrt(3)) * |A-C| * |B-C| * e
    //
    // where e = 0.5 * DBL_EPSILON.  If the determinant magnitude is larger than
    // this value then we know its sign with certainty.
    const DET_ERROR_MULTIPLIER: f64 = 3.2321 * f64::EPSILON; // see above
    let (det, max_error) = if ab2 >= bc2 && ab2 >= ca2 {
        // AB is the longest edge, so compute (A-C)x(B-C).C.
        let det = -dot(&cross(&ca, &bc), c);
        let max_error = DET_ERROR_MULTIPLIER * (ca2 * bc2).sqrt();
        (det, max_error)
    } else if bc2 >= ca2 {
        // BC is the longest edge, so compute (B-A)x(C-A).A.
        let det = -dot(&cross(&ab, &ca), a);
        let max_error = DET_ERROR_MULTIPLIER * (ab2 * ca2).sqrt();
        (det, max_error)
    } else {
        // CA is the longest edge, so compute (C-B)x(A-B).B.
        let det = -dot(&cross(&bc, &ab), b);
        let max_error = DET_ERROR_MULTIPLIER * (bc2 * ab2).sqrt();
        (det, max_error)
    };

    // Precomputed: 3.2321 * f64::EPSILON * f64::MIN_POSITIVE.sqrt()
    // = 7.176703675781937e-16 * 1.4916681462400413e-154
    const MIN_NO_UNDERFLOW_ERROR: f64 = 1.0705260268167732e-169;
    if max_error < MIN_NO_UNDERFLOW_ERROR {
        return 0;
    }

    if det.abs() <= max_error {
        0
    } else if det > 0.0 {
        1
    } else {
        -1
    }
}

/// Given two edges AB and CD where at least two vertices are identical
/// (i.e. CrossingSign(a,b,c,d) == 0), this function defines whether the two edges "cross" in such
/// a way that point-in-polygon containment tests can be implemented by counting the number of edge
/// crossings.  The basic rule is that a "crossing" occurs if AB is encountered after CD during a
/// CCW sweep around the shared vertex starting from a fixed reference point.
///
/// Note that according to this rule, if AB crosses CD then in general CD does not cross AB.
/// However, this leads to the correct result when counting polygon edge crossings.  For example,
/// suppose that A,B,C are three consecutive vertices of a CCW polygon.  If we now consider the
/// edge crossings of a segment BP as P sweeps around B, the crossing number
/// changes parity exactly when BP crosses BA or BC.
///
/// Useful properties of VertexCrossing (VC):
///
/// (1) VC(a,a,c,d) == VC(a,b,c,c) == false
/// (2) VC(a,b,a,b) == VC(a,b,b,a) == true
/// (3) VC(a,b,c,d) == VC(a,b,d,c) == VC(b,a,c,d) == VC(b,a,d,c)
/// (3) If exactly one of a,b equals one of c,d, then exactly one of
///      VC(a,b,c,d) and VC(c,d,a,b) is true
///
/// It is an error to call this method with 4 distinct vertices.
pub fn vertex_crossing(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], d: &[f64; 3]) -> bool {
    if a == b || c == d {
        return false;
    }

    if a == c {
        return (b == d) || ordered_ccw(&ref_dir(a), d, b, a);
    }
    if b == d {
        return ordered_ccw(&ref_dir(b), c, a, b);
    }
    if a == d {
        return (b == c) || ordered_ccw(&ref_dir(a), c, b, a);
    }
    if b == c {
        return ordered_ccw(&ref_dir(b), d, a, b);
    }

    assert!(false, "vertex_crossing called with 4 distinct vertices");
    false
}

/// Efficiently tests edges against a fixed edge AB.
///
/// This is especially efficient when testing against an edge chain connecting vertices
/// v0, v1, v2, ... Use restart_at() to jump to a new location in the chain.
#[derive(Clone)]
pub struct S2EdgeCrosser {
    // Fixed edge AB
    a: [f64; 3],
    b: [f64; 3],
    a_cross_b: [f64; 3],

    // Tangent planes for fast rejection (computed lazily)
    have_tangents: bool,
    a_tangent: [f64; 3],
    b_tangent: [f64; 3],

    // Edge chain state
    c: [f64; 3],
    acb: i32, // Orientation of triangle ACB

    // Temporary for CrossingSignInternal
    bda: i32, // Orientation of triangle BDA
}

impl S2EdgeCrosser {
    /// Creates a new edge crosser for testing edges against the fixed edge AB.
    pub fn new(a: &[f64; 3], b: &[f64; 3]) -> Self {
        assert!((norm2(a) - 1.0).abs() < 5.0 * f64::EPSILON);
        assert!((norm2(b) - 1.0).abs() < 5.0 * f64::EPSILON);

        Self {
            a: *a,
            b: *b,
            a_cross_b: cross(a, b),
            have_tangents: false,
            a_tangent: [0.0, 0.0, 0.0],
            b_tangent: [0.0, 0.0, 0.0],
            c: [0.0, 0.0, 0.0],
            acb: 0,
            bda: 0,
        }
    }

    /// Returns the first endpoint of the fixed edge.
    pub fn a(&self) -> &[f64; 3] {
        &self.a
    }

    /// Returns the second endpoint of the fixed edge.
    pub fn b(&self) -> &[f64; 3] {
        &self.b
    }

    /// Returns the current chain vertex C.
    pub fn c(&self) -> &[f64; 3] {
        &self.c
    }

    /// Tests whether the fixed edge AB crosses edge CD.
    ///
    /// This is the two-argument version for testing disconnected edges.
    pub fn crossing_sign_two(&mut self, c: &[f64; 3], d: &[f64; 3]) -> i32 {
        if *c != self.c {
            self.restart_at(c);
        }
        self.crossing_sign(d)
    }

    /// Tests whether the fixed edge AB crosses edge CD using the vertex crossing rule.
    pub fn edge_or_vertex_crossing_two(&mut self, c: &[f64; 3], d: &[f64; 3]) -> bool {
        if *c != self.c {
            self.restart_at(c);
        }
        self.edge_or_vertex_crossing(d)
    }

    /// Sets the current chain vertex to C.
    ///
    /// Call this to "jump" to a new location in an edge chain.
    pub fn restart_at(&mut self, c: &[f64; 3]) {
        assert!((norm2(c) - 1.0).abs() < 5.0 * f64::EPSILON);
        self.c = *c;
        self.acb = -triage_sign(&self.a, &self.b, c, &self.a_cross_b);
    }

    /// Tests whether the fixed edge AB crosses edge CD, where C is the previous vertex from the
    /// last call.
    ///
    /// This is the single-argument chain version. Use `restart_at` to set C.
    pub fn crossing_sign(&mut self, d: &[f64; 3]) -> i32 {
        assert!((norm2(d) - 1.0).abs() < 5.0 * f64::EPSILON);

        // Fast path: test if BDA has opposite orientation from ACB
        // TriageSign is invariant under rotation, so ABD has same orientation as BDA
        let bda = triage_sign(&self.a, &self.b, d, &self.a_cross_b);
        if self.acb == -bda && bda != 0 {
            // Most common case: C and D are on the same side of AB
            // Save D as the new C for the next call
            self.c = *d;
            self.acb = -bda;
            return -1;
        }
        self.bda = bda;
        self.crossing_sign_internal(d)
    }

    /// Returns the crossing sign from the last interior crossing.
    ///
    /// When `crossing_sign` returns +1, this returns the sign of the crossing:
    /// +1 if AB crosses CD from left to right, -1 if right to left.
    pub fn last_interior_crossing_sign(&self) -> i32 {
        // The crossing sign equals Sign(ABC) which equals acb with sign flipped
        self.acb
    }

    /// Internal implementation for when fast path doesn't work.
    fn crossing_sign_internal(&mut self, d: &[f64; 3]) -> i32 {
        // At this point it is still very likely that CD does not cross AB.  Two common situations
        // are (1) CD crosses the great circle through AB but does not cross AB itself, or (2)
        // A,B,C,D are four points on a line such that AB does not overlap CD.  For example, the
        // latter happens when a line or curve is sampled finely, or when geometry is constructed
        // by computing the union of S2CellIds.
        //
        // Most of the time, we can determine that AB and CD do not intersect by computing the two
        // outward-facing tangents at A and B (parallel to AB) and testing whether AB and CD are on
        // opposite sides of the plane perpendicular to one of these tangents.  This is somewhat
        // expensive but still much cheaper than s2pred::ExpensiveSign.
        if !self.have_tangents {
            let norm = robust_cross(&self.a, &self.b);
            self.a_tangent = cross(&self.a, &norm);
            self.b_tangent = cross(&norm, &self.b);
            self.have_tangents = true;
        }

        // The error in RobustCrossProd() is insignificant.  The maximum error in the call to
        // CrossProd() (i.e., the maximum norm of the error vector) is
        // (0.5 + 1/sqrt(3)) * DBL_EPSILON.  The maximum error in each call to DotProd() below is
        // DBL_EPSILON.  (There is also a small relative error term that is insignificant because
        // we are comparing the result against a constant that is very close to zero.)
        if (dot(&self.c, &self.a_tangent) > TANGENT_ERROR
            && dot(d, &self.a_tangent) > TANGENT_ERROR)
            || (dot(&self.c, &self.b_tangent) > TANGENT_ERROR
                && dot(d, &self.b_tangent) > TANGENT_ERROR)
        {
            self.c = *d;
            self.acb = -self.bda;
            return -1;
        }

        // Otherwise, eliminate the cases where two vertices from different edges are equal.
        // (These cases could be handled in the code below, but we would rather avoid calling
        // ExpensiveSign whenever possible.)
        if self.a == self.c || self.a == *d || self.b == self.c || self.b == *d {
            self.c = *d;
            self.acb = -self.bda;
            return 0;
        }

        // Eliminate cases where an input edge is degenerate.  (Note that in most cases, if CD is
        // degenerate then this method is not even called because acb_ and bda have different
        // signs.)
        if self.a == self.b || self.c == *d {
            self.c = *d;
            self.acb = -self.bda;
            return -1;
        }

        // Otherwise it's time to break out the big guns.
        if self.acb == 0 {
            self.acb = -expensive_sign(&self.a, &self.b, &self.c, true);
        }
        assert!(self.acb != 0);

        if self.bda == 0 {
            self.bda = expensive_sign(&self.a, &self.b, d, true);
        }
        assert!(self.bda != 0);

        if self.bda != self.acb {
            self.c = *d;
            self.acb = -self.bda;
            return -1;
        }

        let c_cross_d = cross(&self.c, d);
        let cbd = -sign_with_cross(&self.c, d, &self.b, &c_cross_d);
        if cbd != self.acb {
            self.c = *d;
            self.acb = -self.bda;
            return -1;
        }

        let dac = sign_with_cross(&self.c, d, &self.a, &c_cross_d);
        let result = if dac != self.acb { -1 } else { 1 };

        self.c = *d;
        self.acb = -self.bda;
        result
    }

    /// Tests edge crossing with vertex crossing rule included.
    pub fn edge_or_vertex_crossing(&mut self, d: &[f64; 3]) -> bool {
        // Save c since crossing_sign will overwrite it
        let c = self.c;
        let crossing = self.crossing_sign(d);
        if crossing < 0 {
            return false;
        }
        if crossing > 0 {
            return true;
        }
        vertex_crossing(&self.a, &self.b, &c, d)
    }
}
