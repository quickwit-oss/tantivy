//! Exact arithmetic predicates for spherical geometry.
//!
//! This module provides robust geometric predicates that handle degenerate cases using exact
//! arithmetic and symbolic perturbation.
//!
//! s2predicates.cc

use num_rational::BigRational;
use num_traits::{FromPrimitive, Signed};

use super::math::cross;

// Exact arithmetic types and operations for the ExactSign path.
//
// Every f64 is exactly representable as a rational number (mantissa times a power of two), so
// BigRational::from_f64 loses nothing. Multiply two rationals, get an exact rational. Add them,
// exact. Check sign, exact.
//
// These replace C++ ExactFloat / Vector3_xf from exactfloat.h.

pub(super) type ExactPoint = [BigRational; 3];

pub(super) fn to_exact(p: &[f64; 3]) -> ExactPoint {
    [
        BigRational::from_f64(p[0]).unwrap(),
        BigRational::from_f64(p[1]).unwrap(),
        BigRational::from_f64(p[2]).unwrap(),
    ]
}

pub(super) fn exact_cross(a: &ExactPoint, b: &ExactPoint) -> ExactPoint {
    [
        &a[1] * &b[2] - &a[2] * &b[1],
        &a[2] * &b[0] - &a[0] * &b[2],
        &a[0] * &b[1] - &a[1] * &b[0],
    ]
}

fn exact_dot(a: &ExactPoint, b: &ExactPoint) -> BigRational {
    &a[0] * &b[0] + &a[1] * &b[1] + &a[2] * &b[2]
}

fn exact_sgn(x: &BigRational) -> i32 {
    if x.is_positive() {
        1
    } else if x.is_negative() {
        -1
    } else {
        0
    }
}

/// Uses arbitrary-precision arithmetic and the "simulation of simplicity" technique in order to be
/// completely robust (i.e., to return consistent results for all possible inputs).
pub fn expensive_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], perturb: bool) -> i32 {
    // Return zero if and only if two points are the same.  This ensures (1).
    if a == b || b == c || c == a {
        return 0;
    }

    // Next we try recomputing the determinant still using floating-point arithmetic but in a more
    // precise way.  This is more expensive than the simple calculation done by TriageSign(), but
    // it is still *much* cheaper than using arbitrary-precision arithmetic.  This optimization is
    // able to compute the correct determinant sign in virtually all cases except when the three
    // points are truly collinear (e.g., three points on the equator).
    let det_sign = stable_sign(a, b, c);
    if det_sign != 0 {
        return det_sign;
    }

    // Otherwise fall back to exact arithmetic and symbolic permutations.
    exact_sign(a, b, c, perturb)
}

// Compute the determinant using exact arithmetic and/or symbolic permutations.  Requires that the
// three points are distinct.
fn exact_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], perturb: bool) -> i32 {
    assert!(a != b && b != c && c != a);

    // Sort the three points in lexicographic order, keeping track of the sign of the permutation.
    // (Each exchange inverts the sign of the determinant.)
    let mut points = [*a, *b, *c];
    let perm_sign = sort_with_parity(&mut points);

    // Construct multiple-precision versions of the sorted points and compute their exact 3x3
    // determinant.
    let xa = to_exact(&points[0]);
    let xb = to_exact(&points[1]);
    let xc = to_exact(&points[2]);
    let xb_cross_xc = exact_cross(&xb, &xc);
    let det = exact_dot(&xa, &xb_cross_xc);

    // C++ checks !isnan(det) and det.prec() < det.max_prec() here.
    // BigRational cannot produce NaN or overflow, so these are unnecessary.

    // If the exact determinant is non-zero, we're done.
    let det_sign = exact_sgn(&det);
    if det_sign == 0 && perturb {
        // Otherwise, we need to resort to symbolic perturbations to resolve the
        // sign of the determinant.
        let det_sign = symbolically_perturbed_sign(&xa, &xb, &xc, &xb_cross_xc);
        assert_ne!(0, det_sign);
        perm_sign * det_sign
    } else {
        perm_sign * det_sign
    }
}

// Compute the determinant in a numerically stable way.  Unlike TriageSign(), this method can
// usually compute the correct determinant sign even when all three points are as collinear as
// possible.  For example if three points are spaced 1km apart along a random line on the Earth's
// surface using the nearest representable points, there is only a 0.4% chance that this method
// will not be able to find the determinant sign.  The probability of failure decreases as the
// points get closer together; if the collinear points are
// 1 meter apart, the failure rate drops to 0.0004%.
//
// This method could be extended to also handle nearly-antipodal points (and in fact an earlier
// version of this code did exactly that), but antipodal points are rare in practice so it seems
// better to simply fall back to exact arithmetic in that case.
fn stable_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> i32 {
    let ab = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    let bc = [c[0] - b[0], c[1] - b[1], c[2] - b[2]];
    let ca = [a[0] - c[0], a[1] - c[1], a[2] - c[2]];
    let ab2 = ab[0] * ab[0] + ab[1] * ab[1] + ab[2] * ab[2];
    let bc2 = bc[0] * bc[0] + bc[1] * bc[1] + bc[2] * bc[2];
    let ca2 = ca[0] * ca[0] + ca[1] * ca[1] + ca[2] * ca[2];

    // Now compute the determinant ((A-C)x(B-C)).C, where the vertices have been cyclically
    // permuted if necessary so that AB is the longest edge.  (This minimizes the magnitude of
    // cross product.)  At the same time we also compute the maximum error in the determinant.
    // Using a similar technique to the one used for kMaxDetError, the error is at most
    //
    //   |d| <= (3 + 6/sqrt(3)) * |A-C| * |B-C| * e
    //
    // where e = 0.5 * DBL_EPSILON.  If the determinant magnitude is larger than this value then we
    // know its sign with certainty.
    const DET_ERROR_MULTIPLIER: f64 = 3.2321 * f64::EPSILON; // see above
    let (det, max_error) = if ab2 >= bc2 && ab2 >= ca2 {
        // AB is the longest edge, so compute (A-C)x(B-C).C.
        let cross = cross(&ca, &bc);
        let det = -(cross[0] * c[0] + cross[1] * c[1] + cross[2] * c[2]);
        let max_error = DET_ERROR_MULTIPLIER * (ca2 * bc2).sqrt();
        (det, max_error)
    } else if bc2 >= ca2 {
        // BC is the longest edge, so compute (B-A)x(C-A).A.
        let cross = cross(&ab, &ca);
        let det = -(cross[0] * a[0] + cross[1] * a[1] + cross[2] * a[2]);
        let max_error = DET_ERROR_MULTIPLIER * (ab2 * ca2).sqrt();
        (det, max_error)
    } else {
        // CA is the longest edge, so compute (C-B)x(A-B).B.
        let cross = cross(&bc, &ab);
        let det = -(cross[0] * b[0] + cross[1] * b[1] + cross[2] * b[2]);
        let max_error = DET_ERROR_MULTIPLIER * (bc2 * ab2).sqrt();
        (det, max_error)
    };

    // Errors smaller than this value may not be accurate due to underflow.
    const MIN_NO_UNDERFLOW_ERROR: f64 = 3.2321 * f64::EPSILON * 1.4916681462400413e-154; // sqrt(DBL_MIN)
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

/// The following function returns the sign of the determinant of three points A, B, C under a
/// model where every possible S2Point is slightly perturbed by a unique infinitesmal amount such
/// that no three perturbed points are collinear and no four points are coplanar.  The
/// perturbations are so small that they do not change the sign of any determinant that was
/// non-zero before the perturbations, and therefore can be safely ignored unless the determinant
/// of three points is exactly zero (using multiple-precision arithmetic).
///
/// Since the symbolic perturbation of a given point is fixed (i.e., the perturbation is the same
/// for all calls to this method and does not depend on the other two arguments), the results of
/// this method are always self-consistent.  It will never return results that would correspond to
/// an "impossible" configuration of non-degenerate points.
///
/// Requirements:
/// - The 3x3 determinant of A, B, C must be exactly zero.
/// - The points must be distinct, with A < B < C in lexicographic order.
///
/// Returns:
/// - +1 or -1 according to the sign of the determinant after the symbolic perturbations are taken
///   into account.
///
/// Reference:
/// - "Simulation of Simplicity" (Edelsbrunner and Muecke, ACM Transactions on Graphics, 1990).
fn symbolically_perturbed_sign(
    a: &ExactPoint,
    b: &ExactPoint,
    c: &ExactPoint,
    b_cross_c: &ExactPoint,
) -> i32 {
    // This method requires that the points are sorted in lexicographically increasing order.  This
    // is because every possible S2Point has its own symbolic perturbation such that if A < B then
    // the symbolic perturbation for A is much larger than the perturbation for B.
    //
    // Alternatively, we could sort the points in this method and keep track of the sign of the
    // permutation, but it is more efficient to do this before converting the inputs to the
    // multi-precision representation, and this also lets us re-use the result of the
    // cross product B x C.
    assert!(a < b && b < c);

    let det_sign = exact_sgn(&b_cross_c[2]); // da[2]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&b_cross_c[1]); // da[1]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&b_cross_c[0]); // da[0]
    if det_sign != 0 {
        return det_sign;
    }

    let det_sign = exact_sgn(&(&c[0] * &a[1] - &c[1] * &a[0])); // db[2]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&c[0]); // db[2] * da[1]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = -exact_sgn(&c[1]); // db[2] * da[0]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&(&c[2] * &a[0] - &c[0] * &a[2])); // db[1]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&c[2]); // db[1] * da[0]
    if det_sign != 0 {
        return det_sign;
    }

    // The following test is listed in the paper, but it is redundant because
    // the previous tests guarantee that C == (0, 0, 0).
    assert_eq!(0, exact_sgn(&(&c[1] * &a[2] - &c[2] * &a[1]))); // db[0]

    let det_sign = exact_sgn(&(&a[0] * &b[1] - &a[1] * &b[0])); // dc[2]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = -exact_sgn(&b[0]); // dc[2] * da[1]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&b[1]); // dc[2] * da[0]
    if det_sign != 0 {
        return det_sign;
    }
    let det_sign = exact_sgn(&a[0]); // dc[2] * db[1]
    if det_sign != 0 {
        return det_sign;
    }

    1 // dc[2] * db[1] * da[0]
}

// Sort the three points in lexicographic order, keeping track of the sign of the permutation.
// (Each exchange inverts the sign of the determinant.)
fn sort_with_parity(points: &mut [[f64; 3]; 3]) -> i32 {
    let mut parity = 1;

    if points[1] < points[0] {
        points.swap(0, 1);
        parity = -parity;
    }
    if points[2] < points[1] {
        points.swap(1, 2);
        parity = -parity;
    }
    if points[1] < points[0] {
        points.swap(0, 1);
        parity = -parity;
    }

    parity
}
