//! Loop measure functions on the unit sphere.
//!
//! s2loop_measures.cc, s2measures.cc

use std::f64::consts::PI;

use super::crossings::{robust_cross, sign};
use super::math::{cross, dot};

/// Returns the angle between two vectors. Uses atan2(|AxB|, A.B).
///
/// Vector3::Angle in util/math/vector.h
fn angle(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    let c = cross(a, b);
    let cross_norm = (c[0] * c[0] + c[1] * c[1] + c[2] * c[2]).sqrt();
    cross_norm.atan2(dot(a, b))
}

/// Returns the exterior angle at vertex B in the triangle ABC.  The return
/// value is positive if ABC is counterclockwise and negative otherwise.  If
/// you imagine an ant walking from A to B to C, this is the angle that the
/// ant turns at vertex B (positive = left = CCW, negative = right = CW).
///
/// Ensures that TurnAngle(a,b,c) == -TurnAngle(c,b,a) for all distinct
/// a,b,c. The result is undefined if (a == b || b == c), but is either
/// -Pi or Pi if (a == c).
///
/// s2measures.cc
pub fn turn_angle(a: &[f64; 3], b: &[f64; 3], c: &[f64; 3]) -> f64 {
    // We use RobustCrossProd() to get good accuracy when two points are very
    // close together, and Sign() to ensure that the sign is correct for
    // turns that are close to 180 degrees.
    //
    // Unfortunately we can't save RobustCrossProd(a, b) and pass it as the
    // optional 4th argument to Sign(), because Sign() requires a.CrossProd(b)
    // exactly (the robust version differs in magnitude).
    let angle = angle(&robust_cross(a, b), &robust_cross(b, c));

    // Don't return Sign() * angle because it is legal to have (a == c).
    if sign(a, b, c) > 0 {
        angle
    } else {
        -angle
    }
}

/// LoopOrder represents a cyclic ordering of the loop vertices, starting at
/// the index "first" and proceeding in direction "dir" (either +1 or -1).
/// "first" and "dir" must be chosen such that (first, ..., first + n * dir)
/// are all in the range [0, 2*n-1] as required by the loop indexing below.
#[derive(Clone, Copy, PartialEq, Eq)]
struct LoopOrder {
    first: i32,
    dir: i32,
}

/// Index into a loop with wraparound. Indices in [0, 2*n-1] are mapped to
/// [0, n-1] by subtracting n when >= n.
#[inline]
fn loop_index(i: i32, n: i32) -> usize {
    let j = i - n;
    (if j < 0 { i } else { j }) as usize
}

fn is_order_less(order1: LoopOrder, order2: LoopOrder, loop_vertices: &[[f64; 3]]) -> bool {
    if order1 == order2 {
        return false;
    }
    let n = loop_vertices.len() as i32;
    let mut i1 = order1.first;
    let mut i2 = order2.first;
    let dir1 = order1.dir;
    let dir2 = order2.dir;
    debug_assert_eq!(
        loop_vertices[loop_index(i1, n)],
        loop_vertices[loop_index(i2, n)]
    );
    let mut remaining = n;
    while {
        remaining -= 1;
        remaining
    } > 0
    {
        i1 += dir1;
        i2 += dir2;
        if loop_vertices[loop_index(i1, n)] < loop_vertices[loop_index(i2, n)] {
            return true;
        }
        if loop_vertices[loop_index(i1, n)] > loop_vertices[loop_index(i2, n)] {
            return false;
        }
    }
    false
}

/// Returns an index "first" and a direction "dir" such that the vertex
/// sequence (first, first + dir, ..., first + (n - 1) * dir) does not change
/// when the loop vertex order is rotated or reversed.  This allows the loop
/// vertices to be traversed in a canonical order.
fn get_canonical_loop_order(loop_vertices: &[[f64; 3]]) -> LoopOrder {
    // In order to handle loops with duplicate vertices and/or degeneracies, we
    // return the LoopOrder that minimizes the entire corresponding vertex
    // *sequence*.  For example, suppose that vertices are sorted
    // alphabetically, and consider the loop CADBAB.  The canonical loop order
    // would be (4, 1), corresponding to the vertex sequence ABCADB.  (For
    // comparison, loop order (4, -1) yields the sequence ABDACB.)
    //
    // If two or more loop orders yield identical minimal vertex sequences, then
    // it doesn't matter which one we return (since they yield the same result).

    // For efficiency, we divide the process into two steps.  First we find the
    // smallest vertex, and the set of vertex indices where that vertex occurs
    // (noting that the loop may contain duplicate vertices).  Then we consider
    // both possible directions starting from each such vertex index, and return
    // the LoopOrder corresponding to the smallest vertex sequence.
    let n = loop_vertices.len() as i32;
    if n == 0 {
        return LoopOrder { first: 0, dir: 1 };
    }

    let mut min_indices: Vec<i32> = vec![0];
    for i in 1..n {
        if loop_vertices[i as usize] <= loop_vertices[min_indices[0] as usize] {
            if loop_vertices[i as usize] < loop_vertices[min_indices[0] as usize] {
                min_indices.clear();
            }
            min_indices.push(i);
        }
    }
    let mut min_order = LoopOrder {
        first: min_indices[0],
        dir: 1,
    };
    for &min_index in &min_indices {
        let order1 = LoopOrder {
            first: min_index,
            dir: 1,
        };
        let order2 = LoopOrder {
            first: min_index + n,
            dir: -1,
        };
        if is_order_less(order1, min_order, loop_vertices) {
            min_order = order1;
        }
        if is_order_less(order2, min_order, loop_vertices) {
            min_order = order2;
        }
    }
    min_order
}

/// Remove any degeneracies from the loop. Returns the pruned vertices.
///
/// Removes AA (duplicate consecutive vertices) and ABA (whiskers). The
/// pruning wraps around the loop boundary.
fn prune_degeneracies(loop_vertices: &[[f64; 3]]) -> Vec<[f64; 3]> {
    let mut vertices: Vec<[f64; 3]> = Vec::with_capacity(loop_vertices.len());
    // Move vertices from loop to vertices, checking for degeneracies as we
    // go.  Invariant: the partially constructed sequence vertices contains no
    // AAs nor ABAs.
    for &v in loop_vertices {
        if !vertices.is_empty() {
            if v == *vertices.last().unwrap() {
                // De-dup: AA -> A.
                continue;
            }
            if vertices.len() >= 2 && v == vertices[vertices.len() - 2] {
                // Remove whisker: ABA -> A.
                vertices.pop();
                continue;
            }
        }
        // The new vertex isn't involved in a degeneracy involving earlier vertices.
        vertices.push(v);
    }
    if vertices.len() >= 2 && vertices[0] == *vertices.last().unwrap() {
        // Remove AA that wraps from end to beginning.
        vertices.pop();
    }

    // Invariant from this point on: there are no more AA's (not even wrapped),
    // and no transformations we do after this (ABA -> A) introduce any AA's.

    // Check whether the loop was completely degenerate.
    if vertices.len() < 3 {
        return Vec::new();
    }

    // Otherwise some portion of the loop is guaranteed to be non-degenerate
    // (this requires some thought).
    // However there may still be some ABA->A's to do at the ends.

    // If the loop begins with BA and ends with A, or begins with A and ends with
    // AB, then there is an edge pair of the form ABA including the first and last
    // point, which we remove by removing the first and last point, leaving A at
    // the beginning or end.  Do this as many times as we can.
    // As noted above, this is guaranteed to leave a non-degenerate loop.
    let mut k = 0;
    while vertices[k + 1] == vertices[vertices.len() - 1 - k]
        || vertices[k] == vertices[vertices.len() - 2 - k]
    {
        k += 1;
    }
    if k > 0 {
        vertices.drain(..k);
        vertices.truncate(vertices.len() - k);
    }
    vertices
}

/// Returns the sum of the turning angles at each vertex in the loop. The
/// return value is positive if the loop is counter-clockwise.
///
/// Degenerate loops are handled by ignoring them.  Specifically, if a loop
/// can be expressed as the concatenation of degenerate and non-degenerate
/// loops, then its curvature is defined as the curvature of the
/// non-degenerate portion.
///
/// For any loop, the curvature is an integer multiple of 2*Pi.  In
/// particular:
///
///   - For a non-degenerate loop with no self-intersections that encloses a
///     small region, the curvature is approximately +2*Pi.
///
///   - For a non-degenerate loop with no self-intersections that encloses a
///     large region (more than half the sphere), the curvature is
///     approximately -2*Pi.
///
///   - For a loop that makes one complete clockwise revolution, the
///     curvature is approximately -2*Pi * (1 + area / 2*Pi).
///
///   For any such loop, reversing the order of the vertices is guaranteed to
///   negate the curvature.  This property can be used to define a unique
///   normalized orientation for every loop.
///
/// s2loop_measures.cc
pub fn get_curvature(loop_vertices: &[[f64; 3]]) -> f64 {
    // By convention, a loop with no vertices contains all points on the sphere.
    if loop_vertices.is_empty() {
        return -2.0 * PI;
    }

    // Remove any degeneracies from the loop.
    let pruned = prune_degeneracies(loop_vertices);

    // If the entire loop was degenerate, its turning angle is defined as 2*Pi.
    if pruned.is_empty() {
        return 2.0 * PI;
    }

    // To ensure that we get the same result when the vertex order is rotated,
    // and that the result is negated when the vertex order is reversed, we need
    // to add up the individual turn angles in a consistent order.  (In general,
    // adding up a set of numbers in a different order can change the sum due to
    // rounding errors.)
    //
    // Furthermore, if we just accumulate an ordinary sum then the worst-case
    // error is quadratic in the number of vertices.  (This can happen with
    // spiral shapes, where the partial sum of the turning angles can be linear
    // in the number of vertices.)  To avoid this we use the Kahan summation
    // algorithm (http://en.wikipedia.org/wiki/Kahan_summation_algorithm).
    let order = get_canonical_loop_order(&pruned);
    let n = pruned.len() as i32;
    let i0 = order.first;
    let dir = order.dir;

    let mut sum = turn_angle(
        &pruned[loop_index((i0 + n - dir) % (2 * n), n)],
        &pruned[loop_index(i0, n)],
        &pruned[loop_index((i0 + dir) % (2 * n), n)],
    );
    let mut compensation = 0.0; // Kahan summation algorithm
    let mut i = i0;
    let mut remaining = n - 1;
    while remaining > 0 {
        i += dir;
        let angle = turn_angle(
            &pruned[loop_index(i - dir, n)],
            &pruned[loop_index(i, n)],
            &pruned[loop_index(i + dir, n)],
        );
        let old_sum = sum;
        let corrected = angle + compensation;
        sum += corrected;
        compensation = (old_sum - sum) + corrected;
        remaining -= 1;
    }
    let k_max_curvature = 2.0 * PI - 4.0 * f64::EPSILON;
    sum += compensation;
    sum.clamp(-k_max_curvature, k_max_curvature) * dir as f64
}

/// Returns the maximum error in get_curvature() for the given loop.
///
/// s2loop_measures.cc
pub fn get_curvature_max_error(num_vertices: usize) -> f64 {
    // The maximum error can be bounded as follows:
    //   3.00 * DBL_EPSILON    for RobustCrossProd(b, a)
    //   3.00 * DBL_EPSILON    for RobustCrossProd(c, b)
    //   3.25 * DBL_EPSILON    for Angle()
    //   2.00 * DBL_EPSILON    for each addition in the Kahan summation
    //  -------------------
    //  11.25 * DBL_EPSILON
    let k_max_error_per_vertex = 11.25 * f64::EPSILON;
    k_max_error_per_vertex * num_vertices as f64
}

/// Returns true if the loop area is at most 2*Pi.  (A small amount of error is
/// allowed in order to ensure that loops representing an entire hemisphere are
/// always considered normalized.)
///
/// s2loop_measures.cc
pub fn is_normalized(loop_vertices: &[[f64; 3]]) -> bool {
    // We allow some error so that hemispheres are always considered normalized.
    get_curvature(loop_vertices) >= -get_curvature_max_error(loop_vertices.len())
}
