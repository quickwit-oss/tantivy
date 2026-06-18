//! Planar geometry predicates.
//!
//! 2D equivalents of the spherical predicates in the S2 port. The orientation test uses Shewchuk's
//! adaptive precision orient2d from the robust crate. Everything else is Euclidean arithmetic.

/// Orientation of point C relative to the directed line from A to B. Returns positive if C is to
/// the left (counter-clockwise), negative if to the right (clockwise), zero if collinear. Uses
/// Shewchuk's adaptive precision predicate.
pub fn orient2d(a: &[f64; 2], b: &[f64; 2], c: &[f64; 2]) -> f64 {
    robust::orient2d(
        robust::Coord { x: a[0], y: a[1] },
        robust::Coord { x: b[0], y: b[1] },
        robust::Coord { x: c[0], y: c[1] },
    )
}

/// Returns true if segments AB and CD cross. Does not count shared endpoints as crossings.
pub fn segments_cross(a: &[f64; 2], b: &[f64; 2], c: &[f64; 2], d: &[f64; 2]) -> bool {
    let d1 = orient2d(a, b, c);
    let d2 = orient2d(a, b, d);
    let d3 = orient2d(c, d, a);
    let d4 = orient2d(c, d, b);

    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }

    false
}

/// Returns true if point P is inside the polygon defined by the given vertices. The polygon is
/// assumed closed (first vertex equals last vertex). Uses the ray casting algorithm with orient2d
/// for robust crossing decisions.
pub fn point_in_polygon(p: &[f64; 2], vertices: &[[f64; 2]]) -> bool {
    if vertices.len() < 4 {
        return false;
    }
    let mut inside = false;
    let n = vertices.len() - 1; // last vertex is the closure duplicate
    let mut j = n - 1;
    for i in 0..n {
        let vi = &vertices[i];
        let vj = &vertices[j];
        if ((vi[1] > p[1]) != (vj[1] > p[1])) && (orient2d(vi, vj, p) > 0.0) == (vi[1] > vj[1]) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Minimum distance from point P to segment AB. Euclidean.
pub fn point_to_segment_distance(p: &[f64; 2], a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let dx = b[0] - a[0];
    let dy = b[1] - a[1];
    let len_sq = dx * dx + dy * dy;
    if len_sq == 0.0 {
        let ex = p[0] - a[0];
        let ey = p[1] - a[1];
        return (ex * ex + ey * ey).sqrt();
    }
    let t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);
    let proj_x = a[0] + t * dx;
    let proj_y = a[1] + t * dy;
    let ex = p[0] - proj_x;
    let ey = p[1] - proj_y;
    (ex * ex + ey * ey).sqrt()
}

/// Minimum distance between segments AB and CD. Euclidean.
pub fn segment_to_segment_distance(a: &[f64; 2], b: &[f64; 2], c: &[f64; 2], d: &[f64; 2]) -> f64 {
    if segments_cross(a, b, c, d) {
        return 0.0;
    }
    let d1 = point_to_segment_distance(a, c, d);
    let d2 = point_to_segment_distance(b, c, d);
    let d3 = point_to_segment_distance(c, a, b);
    let d4 = point_to_segment_distance(d, a, b);
    d1.min(d2).min(d3).min(d4)
}

/// Euclidean distance between two points.
pub fn edge_length(a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let dx = b[0] - a[0];
    let dy = b[1] - a[1];
    (dx * dx + dy * dy).sqrt()
}
