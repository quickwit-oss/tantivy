//! Implementation of a unit sphere surface.

use std::sync::LazyLock;

use crate::spatial::containment::{brute_force_contains, compute_origin_inside};
use crate::spatial::crossings::S2EdgeCrosser;
use crate::spatial::math::normalize;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::s2coords::{
    face_uv_to_xyz, get_face, st_to_ij, uv_to_st, valid_face_xyz_to_uv, xyz_to_face_uv,
};
use crate::spatial::s2edge_clipping::clip_to_padded_face;
use crate::spatial::s2loop_measures::is_normalized;
use crate::spatial::surface::Surface;

static HILBERT_START: LazyLock<[f64; 3]> =
    LazyLock::new(|| normalize(&face_uv_to_xyz(0, -1.0, -1.0)));

/// The origin should not be a point that is commonly used in edge tests in order to avoid
/// triggering code to handle degenerate cases.  (This rules out the north and south poles.)  It
/// should also not be on the boundary of any low-level S2Cell for the same reason.
///
/// The point chosen here is about 66km from the north pole towards the East Siberian Sea.  See the
/// unittest for more details.  It is written out explicitly using floating-point literals because
/// the optimizer doesn't seem willing to evaluate Normalize() at compile time.
pub const ORIGIN: [f64; 3] = [
    -0.00999946643502502,
    0.002592454260932412,
    0.999946643502502,
    // x: -0.0099994664350250197,
    // y: 0.0025924542609324121,
    // z: 0.99994664350250195,
];

/// Implementation of a unit sphere surface.
#[derive(Clone)]
pub struct Sphere;

impl Surface for Sphere {
    const DIMENSIONS: usize = 3;
    type Point = [f64; 3];
    type EdgeCrosser = S2EdgeCrosser;

    fn project(lon: f64, lat: f64) -> [f64; 3] {
        let lat = lat.to_radians();
        let lon = lon.to_radians();
        let cos_lat = lat.cos();
        [cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin()]
    }

    #[inline]
    fn hilbert_start() -> Self::Point {
        *HILBERT_START
    }

    fn normalize_ring(ring: &mut Vec<Self::Point>) {
        if !is_normalized(ring) {
            ring.reverse();
        }
    }

    #[inline]
    fn point_from_le_bytes(bytes: &[u8]) -> Self::Point {
        let mut point = [0.0f64; 3];
        for i in 0..3 {
            point[i] = f64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
        }
        point
    }

    #[inline]
    fn edge_length(a: &Self::Point, b: &Self::Point) -> f64 {
        ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2) + (a[2] - b[2]).powi(2)).sqrt()
    }

    #[inline]
    fn origin_inside(ring: &[Self::Point]) -> bool {
        compute_origin_inside(ring)
    }

    const FACE_COUNT: i32 = 6;

    #[inline]
    fn get_face(point: &Self::Point) -> i32 {
        get_face(point)
    }

    #[inline]
    fn point_to_face_uv(face: i32, point: &Self::Point) -> (f64, f64) {
        valid_face_xyz_to_uv(face, point)
    }

    #[inline]
    fn face_uv_to_point(face: i32, u: f64, v: f64) -> Self::Point {
        normalize(&face_uv_to_xyz(face, u, v))
    }

    #[inline]
    fn clip_to_face(
        v0: &Self::Point,
        v1: &Self::Point,
        face: i32,
        padding: f64,
    ) -> Option<([f64; 2], [f64; 2])> {
        clip_to_padded_face(v0, v1, face, padding)
    }

    #[inline]
    fn uv_to_st(u: f64) -> f64 {
        uv_to_st(u)
    }

    #[inline]
    fn st_to_uv(s: f64) -> f64 {
        crate::spatial::s2coords::st_to_uv(s)
    }

    #[inline]
    fn cell_id_from_point(point: &Self::Point) -> S2CellId {
        let (face, u, v) = xyz_to_face_uv(point);
        let i = st_to_ij(uv_to_st(u));
        let j = st_to_ij(uv_to_st(v));
        S2CellId::from_face_ij(face, i, j)
    }

    #[inline]
    fn contains_point(point: &Self::Point, ring: &[Self::Point], origin_inside: bool) -> bool {
        brute_force_contains(point, ring, origin_inside)
    }
}
