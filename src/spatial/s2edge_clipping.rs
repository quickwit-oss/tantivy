//! Clipping geodesic edges to S2 cube faces and 2D edges against rectangles.
use super::crossings::robust_cross;
use super::math::{cross, dot, normalize};
use super::r1interval::R1Interval;
use super::r2rect::R2Rect;
use super::s2coords::face_xyz_to_uvw;

/// Maximum error in vertices returned by face clipping functions.
pub const FACE_CLIP_ERROR_UV_COORD: f64 = 9.0 * std::f64::consts::FRAC_1_SQRT_2 * f64::EPSILON;

/// Maximum error in edge clipping functions.
pub const EDGE_CLIP_ERROR_UV_COORD: f64 = 2.25 * f64::EPSILON;

/// Maximum error in IntersectsRect.
pub const INTERSECTS_RECT_ERROR_UV_DIST: f64 = 3.0 * std::f64::consts::SQRT_2 * f64::EPSILON;

/// Represents an edge AB clipped to an S2 cube face.
#[derive(Clone, Copy, Debug)]
pub struct FaceSegment {
    /// The cube face (0-5).
    pub face: i32,
    /// Endpoint A in (u,v) coordinates.
    pub a: [f64; 2],
    /// Endpoint B in (u,v) coordinates.
    pub b: [f64; 2],
}

/// Clips edge AB to the given cube face, returning the (u,v) coordinates. Returns None if the edge
/// does not intersect the face.
pub fn clip_to_face(a: &[f64; 3], b: &[f64; 3], face: i32) -> Option<([f64; 2], [f64; 2])> {
    clip_to_padded_face(a, b, face, 0.0)
}

/// Clips edge AB to a padded cube face. The face is expanded by `padding` in each direction.
pub fn clip_to_padded_face(
    a_xyz: &[f64; 3],
    b_xyz: &[f64; 3],
    face: i32,
    padding: f64,
) -> Option<([f64; 2], [f64; 2])> {
    assert!(padding >= 0.0);

    // Fast path: both endpoints on the same face.
    let a_face = super::s2coords::get_face(a_xyz);
    let b_face = super::s2coords::get_face(b_xyz);

    if a_face == face && b_face == face {
        let (_, a_u, a_v) = super::s2coords::xyz_to_face_uv(a_xyz);
        let (_, b_u, b_v) = super::s2coords::xyz_to_face_uv(b_xyz);
        return Some(([a_u, a_v], [b_u, b_v]));
    }

    // Convert everything into the (u,v,w) coordinates of the given face.  Note
    // that the cross product *must* be computed in the original (x,y,z)
    // coordinate system because RobustCrossProd (unlike the mathematical cross
    // product) can produce different results in different coordinate systems
    // when one argument is a linear multiple of the other, due to the use of
    // symbolic perturbations.
    let n = face_xyz_to_uvw(face, &robust_cross(a_xyz, b_xyz));
    let a = face_xyz_to_uvw(face, a_xyz);
    let b = face_xyz_to_uvw(face, b_xyz);

    // Scale u and v components to handle padding.
    let scale_uv = 1.0 + padding;
    let scaled_n = [scale_uv * n[0], scale_uv * n[1], n[2]];

    // Check if the line intersects the (padded) face.
    if !intersects_face(&scaled_n) {
        return None;
    }

    let n_norm = normalize(&n);
    let a_tangent = cross(&n_norm, &a);
    let b_tangent = cross(&b, &n_norm);

    // Clip both endpoints.
    let neg_scaled_n = [-scaled_n[0], -scaled_n[1], -scaled_n[2]];
    let (a_uv, a_score) = clip_destination(&b, &a, &neg_scaled_n, &b_tangent, &a_tangent, scale_uv);
    let (b_uv, b_score) = clip_destination(&a, &b, &scaled_n, &a_tangent, &b_tangent, scale_uv);

    if a_score + b_score < 3 {
        Some((a_uv, b_uv))
    } else {
        None
    }
}

// The three functions below all compare a sum (u + v) to a third value w.
// They are implemented in such a way that they produce an exact result even
// though all calculations are done with ordinary floating-point operations.
// Here are the principles on which these functions are based:
//
// A. If u + v < w in floating-point, then u + v < w in exact arithmetic.
//
// B. If u + v < w in exact arithmetic, then at least one of the following
//    expressions is true in floating-point:
//       u + v < w
//       u < w - v
//       v < w - u
//
//    Proof: By rearranging terms and substituting ">" for "<", we can assume
//    that all values are non-negative.  Now clearly "w" is not the smallest
//    value, so assume WLOG that "u" is the smallest.  We want to show that
//    u < w - v in floating-point.  If v >= w/2, the calculation of w - v is
//    exact since the result is smaller in magnitude than either input value,
//    so the result holds.  Otherwise we have u <= v < w/2 and w - v >= w/2
//    (even in floating point), so the result also holds.

fn intersects_face(n: &[f64; 3]) -> bool {
    let u = n[0].abs();
    let v = n[1].abs();
    let w = n[2].abs();
    // |Nu| + |Nv| >= |Nw|
    (v >= w - u) && (u >= w - v)
}

/// Returns true if the line intersects opposite edges of the face.
fn intersects_opposite_edges(n: &[f64; 3]) -> bool {
    let u = n[0].abs();
    let v = n[1].abs();
    let w = n[2].abs();

    // ||Nu| - |Nv|| >= |Nw|
    if (u - v).abs() != w {
        return (u - v).abs() >= w;
    }
    if u >= v {
        u - w >= v
    } else {
        v - w >= u
    }
}

/// Returns the exit axis (0 for u, 1 for v).
fn get_exit_axis(n: &[f64; 3]) -> i32 {
    assert!(intersects_face(n));
    if intersects_opposite_edges(n) {
        if n[0].abs() >= n[1].abs() {
            1
        } else {
            0
        }
    } else {
        let sign = |x: f64| x.is_sign_negative() as i32;
        if (sign(n[0]) ^ sign(n[1]) ^ sign(n[2])) == 0 {
            1
        } else {
            0
        }
    }
}

/// Returns the exit point on the face for the given line.
fn get_exit_point(n: &[f64; 3], axis: i32) -> [f64; 2] {
    if axis == 0 {
        let u = if n[1] > 0.0 { 1.0 } else { -1.0 };
        [u, (-u * n[0] - n[2]) / n[1]]
    } else {
        let v = if n[0] < 0.0 { 1.0 } else { -1.0 };
        [(-v * n[1] - n[2]) / n[0], v]
    }
}
/// Clips a destination point.
fn clip_destination(
    a: &[f64; 3],
    b: &[f64; 3],
    scaled_n: &[f64; 3],
    a_tangent: &[f64; 3],
    b_tangent: &[f64; 3],
    scale_uv: f64,
) -> ([f64; 2], i32) {
    const MAX_SAFE_UV_COORD: f64 = 1.0 - FACE_CLIP_ERROR_UV_COORD;

    // Optimization: if B is within the safe region, use it directly.
    if b[2] > 0.0 {
        let uv = [b[0] / b[2], b[1] / b[2]];
        if uv[0].abs().max(uv[1].abs()) <= MAX_SAFE_UV_COORD {
            return (uv, 0);
        }
    }

    // Find where line AB exits the face.
    let exit_axis = get_exit_axis(scaled_n);
    let exit_pt = get_exit_point(scaled_n, exit_axis);
    let mut uv = [scale_uv * exit_pt[0], scale_uv * exit_pt[1]];
    let p = [uv[0], uv[1], 1.0];

    // Determine if exit point is in the segment.
    let mut score = 0;
    let pa = [p[0] - a[0], p[1] - a[1], p[2] - a[2]];
    if dot(&pa, a_tangent) < 0.0 {
        score = 2; // On wrong side of A
    } else {
        let pb = [p[0] - b[0], p[1] - b[1], p[2] - b[2]];
        if dot(&pb, b_tangent) < 0.0 {
            score = 1; // On wrong side of B
        }
    }

    if score > 0 {
        if b[2] <= 0.0 {
            score = 3; // B cannot be projected
        } else {
            uv = [b[0] / b[2], b[1] / b[2]];
        }
    }

    (uv, score)
}

/// Returns true if edge AB intersects the rectangle.
pub fn intersects_rect(a: [f64; 2], b: [f64; 2], rect: &R2Rect) -> bool {
    // Check bounding box intersection first.
    let bound = R2Rect::from_point_pair(a, b);
    if !rect.intersects(&bound) {
        return false;
    }

    // Check if all four vertices are on the same side of the line.
    // n = ortho(b - a) = [-(b[1]-a[1]), b[0]-a[0]]
    let n = [-(b[1] - a[1]), b[0] - a[0]];
    let i = if n[0] >= 0.0 { 1 } else { 0 };
    let j = if n[1] >= 0.0 { 1 } else { 0 };

    let v_max = rect.get_vertex_ij(i, j);
    let v_min = rect.get_vertex_ij(1 - i, 1 - j);

    // dot(n, v - a)
    let max = n[0] * (v_max[0] - a[0]) + n[1] * (v_max[1] - a[1]);
    let min = n[0] * (v_min[0] - a[0]) + n[1] * (v_min[1] - a[1]);

    (max >= 0.0) && (min <= 0.0)
}

/// Clips edge AB to the rectangle, returning the clipped endpoints. Returns None if there is no
/// intersection.
pub fn clip_edge(a: [f64; 2], b: [f64; 2], clip: &R2Rect) -> Option<([f64; 2], [f64; 2])> {
    let mut bound = R2Rect::from_point_pair(a, b);
    if clip_edge_bound(a, b, clip, &mut bound) {
        let ai = if a[0] > b[0] { 1 } else { 0 };
        let aj = if a[1] > b[1] { 1 } else { 0 };
        Some((
            bound.get_vertex_ij(ai, aj),
            bound.get_vertex_ij(1 - ai, 1 - aj),
        ))
    } else {
        None
    }
}

/// Updates the bounding rectangle of edge AB clipped to the given clip region. Returns false if AB
/// does not intersect the clip region.
pub fn clip_edge_bound(a: [f64; 2], b: [f64; 2], clip: &R2Rect, bound: &mut R2Rect) -> bool {
    let diag = ((a[0] > b[0]) != (a[1] > b[1])) as usize;

    clip_bound_axis(
        a[0],
        b[0],
        &mut bound.x,
        a[1],
        b[1],
        &mut bound.y,
        diag,
        &clip[0],
    ) && clip_bound_axis(
        a[1],
        b[1],
        &mut bound.y,
        a[0],
        b[0],
        &mut bound.x,
        diag,
        &clip[1],
    )
}

/// Helper for clip_edge_bound: clips along one axis.
#[allow(clippy::too_many_arguments)]
fn clip_bound_axis(
    a0: f64,
    b0: f64,
    bound0: &mut R1Interval,
    a1: f64,
    b1: f64,
    bound1: &mut R1Interval,
    diag: usize,
    clip0: &R1Interval,
) -> bool {
    if bound0.lo() < clip0.lo() {
        if bound0.hi() < clip0.lo() {
            return false;
        }
        *bound0 = R1Interval::new(clip0.lo(), bound0.hi());
        if !update_endpoint(bound1, diag, interpolate(clip0.lo(), a0, b0, a1, b1)) {
            return false;
        }
    }
    if bound0.hi() > clip0.hi() {
        if bound0.lo() > clip0.hi() {
            return false;
        }
        *bound0 = R1Interval::new(bound0.lo(), clip0.hi());
        if !update_endpoint(bound1, 1 - diag, interpolate(clip0.hi(), a0, b0, a1, b1)) {
            return false;
        }
    }
    true
}

/// Updates an endpoint of an interval.
fn update_endpoint(bound: &mut R1Interval, end: usize, value: f64) -> bool {
    if end == 0 {
        if bound.hi() < value {
            return false;
        }
        if bound.lo() < value {
            *bound = R1Interval::new(value, bound.hi());
        }
    } else {
        if bound.lo() > value {
            return false;
        }
        if bound.hi() > value {
            *bound = R1Interval::new(bound.lo(), value);
        }
    }
    true
}

/// Interpolates a value along the line from (a0,a1) to (b0,b1).
fn interpolate(x: f64, a: f64, b: f64, a1: f64, b1: f64) -> f64 {
    if a == b {
        assert!(x == a && a1 == b1);
        return a1;
    }
    // Interpolate from the closer endpoint for better accuracy.
    if (a - x).abs() <= (b - x).abs() {
        a1 + (b1 - a1) * ((x - a) / (b - a))
    } else {
        b1 + (a1 - b1) * ((x - b) / (a - b))
    }
}
