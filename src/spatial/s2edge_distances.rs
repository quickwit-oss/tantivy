//! Functions for computing the distance from a point to an edge, and between pairs of edges, on
//! the unit sphere.
//!
//! Port of s2edge_distances.h and s2edge_distances.cc (Eric Veach, Google).

use super::crossings::{robust_cross, S2EdgeCrosser};
use super::math::{cross, dot, norm2, sub};
use super::s1chord_angle::S1ChordAngle;

/// Returns the minimum distance from x to any point on the edge ab as an S1ChordAngle.
/// All arguments should be unit length. The result is very accurate for small distances but may
/// have some numerical error if the distance is large (approximately pi/2 or greater). The case
/// a == b is handled correctly.
///
/// Port of S2::GetDistance.
pub fn get_distance(x: &[f64; 3], a: &[f64; 3], b: &[f64; 3]) -> S1ChordAngle {
    let mut min_dist = S1ChordAngle::from_length2(0.0);
    always_update_min_distance::<true>(x, a, b, &mut min_dist);
    min_dist
}

/// If the distance from x to the edge ab is less than min_dist, updates min_dist and returns
/// true. Otherwise returns false. The case a == b is handled correctly.
///
/// Port of S2::UpdateMinDistance.
pub fn update_min_distance(
    x: &[f64; 3],
    a: &[f64; 3],
    b: &[f64; 3],
    min_dist: &mut S1ChordAngle,
) -> bool {
    always_update_min_distance::<false>(x, a, b, min_dist)
}

/// Returns true if the distance from x to the edge ab is less than limit.
///
/// Port of S2::IsDistanceLess.
pub fn is_distance_less(x: &[f64; 3], a: &[f64; 3], b: &[f64; 3], limit: S1ChordAngle) -> bool {
    let mut limit = limit;
    update_min_distance(x, a, b, &mut limit)
}

/// If the minimum distance from x to ab is attained at an interior point of ab (i.e., not an
/// endpoint), and that distance is less than min_dist, then updates min_dist and returns true.
/// Otherwise returns false.
///
/// Port of S2::UpdateMinInteriorDistance.
pub fn update_min_interior_distance(
    x: &[f64; 3],
    a: &[f64; 3],
    b: &[f64; 3],
    min_dist: &mut S1ChordAngle,
) -> bool {
    let xa2 = norm2(&sub(x, a));
    let xb2 = norm2(&sub(x, b));
    always_update_min_interior_distance::<false>(x, a, b, xa2, xb2, min_dist)
}

/// Returns the maximum error in the result of update_min_distance (and associated functions),
/// assuming that all input points are normalized to within the bounds guaranteed by normalize().
///
/// Port of S2::GetUpdateMinDistanceMaxError.
pub fn get_update_min_distance_max_error(dist: S1ChordAngle) -> f64 {
    f64::max(
        get_update_min_interior_distance_max_error(dist),
        dist.get_s2point_constructor_max_error(),
    )
}

// Port of AlwaysUpdateMinInteriorDistance<always_update>.
fn always_update_min_interior_distance<const ALWAYS_UPDATE: bool>(
    x: &[f64; 3],
    a: &[f64; 3],
    b: &[f64; 3],
    xa2: f64,
    xb2: f64,
    min_dist: &mut S1ChordAngle,
) -> bool {
    // The closest point on AB could either be one of the two vertices (the
    // "vertex case") or in the interior (the "interior case").  Let C = A x B.
    // If X is in the spherical wedge extending from A to B around the axis
    // through C, then we are in the interior case.  Otherwise we are in the
    // vertex case.
    //
    // Check whether we might be in the interior case.  For this to be true, XAB
    // and XBA must both be acute angles.  Checking this condition exactly is
    // expensive, so instead we consider the planar triangle ABX (which passes
    // through the sphere's interior).  The planar angles XAB and XBA are always
    // less than the corresponding spherical angles, so if we are in the
    // interior case then both of these angles must be acute.
    //
    // We check this by computing the squared edge lengths of the planar
    // triangle ABX, and testing whether angles XAB and XBA are both acute using
    // the law of cosines:
    //
    //            | XA^2 - XB^2 | < AB^2      (*)
    //
    // There are two sources of error in the expression above (*).
    let ab2 = norm2(&sub(a, b));
    let max_error = 4.75 * f64::EPSILON * (xa2 + xb2 + ab2) + 8.0 * f64::EPSILON * f64::EPSILON;
    if (xa2 - xb2).abs() >= ab2 + max_error {
        return false;
    }

    // The minimum distance might be to a point on the edge interior.  Let R
    // be closest point to X that lies on the great circle through AB.  Rather
    // than computing the geodesic distance along the surface of the sphere,
    // instead we compute the "chord length" through the sphere's interior.
    let c = robust_cross(a, b);
    let c2 = norm2(&c);
    let x_dot_c = dot(x, &c);
    let x_dot_c2 = x_dot_c * x_dot_c;
    if !ALWAYS_UPDATE && x_dot_c2 > c2 * min_dist.length2() {
        // The closest point on the great circle AB is too far away.
        return false;
    }
    // Otherwise we do the exact, more expensive test for the interior case.
    let cx = cross(&c, x);
    if dot(&sub(a, x), &cx) >= 0.0 || dot(&sub(b, x), &cx) <= 0.0 {
        return false;
    }
    // Compute the squared chord length XR^2 = XQ^2 + QR^2.
    let qr = 1.0 - (norm2(&cx) / c2).sqrt();
    let dist2 = (x_dot_c2 / c2) + (qr * qr);
    if !ALWAYS_UPDATE && dist2 >= min_dist.length2() {
        return false;
    }
    *min_dist = S1ChordAngle::from_length2(dist2);
    true
}

// Port of AlwaysUpdateMinDistance<always_update>.
fn always_update_min_distance<const ALWAYS_UPDATE: bool>(
    x: &[f64; 3],
    a: &[f64; 3],
    b: &[f64; 3],
    min_dist: &mut S1ChordAngle,
) -> bool {
    let xa2 = norm2(&sub(x, a));
    let xb2 = norm2(&sub(x, b));
    if always_update_min_interior_distance::<ALWAYS_UPDATE>(x, a, b, xa2, xb2, min_dist) {
        return true; // Minimum distance is attained along the edge interior.
    }
    // Otherwise the minimum distance is to one of the endpoints.
    let dist2 = f64::min(xa2, xb2);
    if !ALWAYS_UPDATE && dist2 >= min_dist.length2() {
        return false;
    }
    *min_dist = S1ChordAngle::from_length2(dist2);
    true
}

/// Like update_min_distance(), but computes the minimum distance between the
/// given pair of edges.  (If the two edges cross, the distance is zero.)
/// The cases a0 == a1 and b0 == b1 are handled correctly.
///
/// Port of S2::UpdateEdgePairMinDistance in s2edge_distances.cc.
pub fn update_edge_pair_min_distance(
    a0: &[f64; 3],
    a1: &[f64; 3],
    b0: &[f64; 3],
    b1: &[f64; 3],
    min_dist: &mut S1ChordAngle,
) -> bool {
    if *min_dist == S1ChordAngle::zero() {
        return false;
    }
    if crossing_sign(a0, a1, b0, b1) >= 0 {
        *min_dist = S1ChordAngle::zero();
        return true;
    }
    // Otherwise, the minimum distance is achieved at an endpoint of at least
    // one of the two edges.  We use "|" rather than "||" below to ensure that
    // all four possibilities are always checked.
    //
    // The calculation below computes each of the six vertex-vertex distances
    // twice (this could be optimized).
    let a = update_min_distance(a0, b0, b1, min_dist);
    let b = update_min_distance(a1, b0, b1, min_dist);
    let c = update_min_distance(b0, a0, a1, min_dist);
    let d = update_min_distance(b1, a0, a1, min_dist);
    a | b | c | d
}

/// Returns true if the minimum distance between two edges is less than distance.
///
/// Port of S2::IsEdgePairDistanceLess in s2edge_distances.cc.
pub fn is_edge_pair_distance_less(
    a0: &[f64; 3],
    a1: &[f64; 3],
    b0: &[f64; 3],
    b1: &[f64; 3],
    distance: S1ChordAngle,
) -> bool {
    // If the edges cross or share an endpoint, the minimum distance is zero.
    if crossing_sign(a0, a1, b0, b1) >= 0 {
        return distance != S1ChordAngle::zero();
    }
    // Otherwise the minimum distance is achieved at an endpoint of at least
    // one of the endpoints of the two edges.  Written this way for short circuiting.
    is_distance_less(a0, b0, b1, distance)
        || is_distance_less(a1, b0, b1, distance)
        || is_distance_less(b0, a0, a1, distance)
        || is_distance_less(b1, a0, a1, distance)
}

// Convenience wrapper matching S2::CrossingSign in s2edge_crossings.cc.
fn crossing_sign(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3], d: &[f64; 3]) -> i32 {
    let mut crosser = S2EdgeCrosser::new(a, b);
    crosser.crossing_sign_two(c, d)
}

// Port of GetUpdateMinInteriorDistanceMaxError.
fn get_update_min_interior_distance_max_error(dist: S1ChordAngle) -> f64 {
    // If a point is more than 90 degrees from an edge, then the minimum
    // distance is always to one of the endpoints, not to the edge interior.
    if dist >= S1ChordAngle::right() {
        return 0.0;
    }

    let b = f64::min(1.0, 0.5 * dist.length2());
    let a = (b * (2.0 - b)).sqrt();
    ((2.5 + 2.0 * 3.0_f64.sqrt() + 8.5 * a) * a
        + (2.0 + 2.0 * 3.0_f64.sqrt() / 3.0 + 6.5 * (1.0 - b)) * b
        + (23.0 + 16.0 / 3.0_f64.sqrt()) * f64::EPSILON)
        * f64::EPSILON
}
