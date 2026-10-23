//! Planar geometry predicates.
//!
//! 2D equivalents of the spherical predicates in the S2 port. The orientation test uses Shewchuk's
//! adaptive precision orient2d from the robust crate. The exact sign wrapper also handles values
//! outside the adaptive predicate's exponent range.

use num_bigint::{BigInt, Sign};

use crate::spatial::r2rect::R2Rect;

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

/// Exact orientation sign for finite coordinates. Returns -1, 0 or 1 without perturbation.
pub fn orient2d_sign(a: &[f64; 2], b: &[f64; 2], c: &[f64; 2]) -> i32 {
    let mut min_exponent = i32::MAX;
    let mut max_exponent = i32::MIN;
    for &value in a.iter().chain(b).chain(c) {
        assert!(value.is_finite());
        if value == 0.0 {
            continue;
        }
        let bits = value.to_bits() & 0x7fff_ffff_ffff_ffff;
        let biased_exponent = (bits >> 52) as i32;
        let exponent = if biased_exponent == 0 {
            63 - bits.leading_zeros() as i32 - 1074
        } else {
            biased_exponent - 1023
        };
        min_exponent = min_exponent.min(exponent);
        max_exponent = max_exponent.max(exponent);
    }
    if min_exponent == i32::MAX {
        return 0;
    }
    if max_exponent - min_exponent > 400 {
        return exact_orient2d_sign(a, b, c);
    }

    // In this band, differences are multiples of 2^-252 and products of 2^-504.
    // Adaptive error terms cannot underflow, and splitter products cannot overflow.
    let det = if min_exponent >= -200 && max_exponent <= 200 {
        orient2d(a, b, c)
    } else {
        // The spread check above ensures that this interval is nonempty.
        let shift = 0i32.clamp(-200 - min_exponent, 200 - max_exponent);
        // The minimum shift lies in [-823, 874], so this factor is finite and normal.
        let scale = f64::from_bits(((shift + 1023) as u64) << 52);
        orient2d(
            &a.map(|value| value * scale),
            &b.map(|value| value * scale),
            &c.map(|value| value * scale),
        )
    };
    (det > 0.0) as i32 - (det < 0.0) as i32
}

fn exact_orient2d_sign(a: &[f64; 2], b: &[f64; 2], c: &[f64; 2]) -> i32 {
    let parts = [a[0], a[1], b[0], b[1], c[0], c[1]].map(|value| {
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & 0x000f_ffff_ffff_ffff;
        let (significand, exponent) = if exponent == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1u64 << 52), exponent - 1075)
        };
        let significand = if bits >> 63 == 0 {
            significand as i64
        } else {
            -(significand as i64)
        };
        (significand, exponent)
    });
    // The caller has already excluded all-zero input.
    let exponent = parts
        .iter()
        .filter(|(significand, _)| *significand != 0)
        .map(|(_, exponent)| *exponent)
        .min()
        .unwrap();
    // Align to one power of two before subtracting. No rational division is needed.
    let [ax, ay, bx, by, cx, cy] = parts.map(|(significand, power)| {
        if significand == 0 {
            BigInt::from(0)
        } else {
            BigInt::from(significand) << (power - exponent) as usize
        }
    });
    let det = (&ax - &cx) * (&by - &cy) - (&ay - &cy) * (&bx - &cx);
    match det.sign() {
        Sign::Minus => -1,
        Sign::NoSign => 0,
        Sign::Plus => 1,
    }
}

/// Returns whether the closed segment AB meets the closed rectangle. Coordinates must be finite.
/// A segment may be a point, and a rectangle may be empty or have zero width or height.
pub fn segment_intersects_rect(a: &[f64; 2], b: &[f64; 2], rect: &R2Rect) -> bool {
    assert!(a.iter().chain(b).all(|value| value.is_finite()));
    assert!((0..2).all(|axis| rect[axis].lo().is_finite() && rect[axis].hi().is_finite()));
    if rect.is_empty() || !rect.intersects(&R2Rect::from_point_pair(*a, *b)) {
        return false;
    }
    if rect.contains_point(*a) || rect.contains_point(*b) {
        return true;
    }
    for axis in 0..2 {
        let other = 1 - axis;
        for value in [rect[axis].lo(), rect[axis].hi()] {
            if value < a[axis].min(b[axis]) || value > a[axis].max(b[axis]) {
                continue;
            }
            let mut c = [0.0; 2];
            let mut d = [0.0; 2];
            c[axis] = value;
            d[axis] = value;
            c[other] = rect[other].lo();
            d[other] = rect[other].hi();
            // The axis range puts a line intersection on AB. Bounding-box overlap also
            // handles the case where AB lies along this side.
            if orient2d_sign(a, b, &c) * orient2d_sign(a, b, &d) <= 0 {
                return true;
            }
        }
    }
    false
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
/// for robust crossing decisions. Boundary points are classified as if displaced
/// by (e, e^2) for positive symbolic e, matching PlaneEdgeCrosser.
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
        if (vi[1] > p[1]) != (vj[1] > p[1]) {
            let orientation = orient2d(vi, vj, p);
            // A boundary edge is left of the displaced point and crosses its leftward ray.
            if orientation == 0.0 || (orientation > 0.0) == (vi[1] > vj[1]) {
                inside = !inside;
            }
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

#[cfg(test)]
#[path = "../tests/plane_math_tests.rs"]
mod tests;
