//! Vector math utilities for 3D point operations.

/// Returns the dot product of two 3D vectors.
#[inline]
pub fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

/// Returns the unit vector in the same direction as p.
#[inline]
pub fn normalize(p: &[f64; 3]) -> [f64; 3] {
    let norm = (p[0] * p[0] + p[1] * p[1] + p[2] * p[2]).sqrt();
    [p[0] / norm, p[1] / norm, p[2] / norm]
}

/// Returns the negation of a 3D vector.
#[inline]
pub fn neg(p: [f64; 3]) -> [f64; 3] {
    [-p[0], -p[1], -p[2]]
}

/// Returns the squared Euclidean norm of a 3D vector.
#[inline]
pub fn norm2(p: &[f64; 3]) -> f64 {
    p[0] * p[0] + p[1] * p[1] + p[2] * p[2]
}

/// Returns the cross product of two 3D vectors.
#[inline]
pub fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

/// Returns x * 2^exp. Equivalent to C's ldexp.
#[inline]
pub fn ldexp(x: f64, exp: i32) -> f64 {
    x * 2.0_f64.powi(exp)
}

/// IEEE 754 remainder. Equivalent to C's remainder().
#[inline]
pub fn remainder(x: f64, y: f64) -> f64 {
    x - (x / y).round_ties_even() * y
}
