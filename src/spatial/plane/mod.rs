//! Planar surface implementation and predicates for 2D spatial indexing.

pub mod crosser;
pub mod math;

use crate::spatial::surface::Surface;

/// Implementation of a planar surface.
#[derive(Debug, Clone, PartialEq)]
pub struct Plane;

impl Surface for Plane {
    const DIMENSIONS: usize = 2;
    type Point = [f64; 2];
    type EdgeCrosser = crosser::PlaneEdgeCrosser;

    fn project(lon: f64, lat: f64) -> [f64; 2] {
        [lon, lat]
    }

    fn hilbert_start() -> Self::Point {
        [-1.0, -1.0]
    }

    #[inline]
    fn edge_length(a: &Self::Point, b: &Self::Point) -> f64 {
        math::edge_length(a, b)
    }

    fn normalize_ring(_ring: &mut Vec<Self::Point>) {}

    #[inline]
    fn point_from_le_bytes(bytes: &[u8]) -> Self::Point {
        let mut point = [0.0f64; 2];
        for i in 0..2 {
            point[i] = f64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
        }
        point
    }

    fn origin_inside(_ring: &[Self::Point]) -> bool {
        false
    }

    const FACE_COUNT: i32 = 1;

    #[inline]
    fn get_face(_point: &Self::Point) -> i32 {
        0
    }

    #[inline]
    fn point_to_face_uv(_face: i32, point: &Self::Point) -> (f64, f64) {
        (point[0], point[1])
    }

    #[inline]
    fn face_uv_to_point(_face: i32, u: f64, v: f64) -> Self::Point {
        [u, v]
    }

    #[inline]
    fn clip_to_face(
        v0: &Self::Point,
        v1: &Self::Point,
        face: i32,
        _padding: f64,
    ) -> Option<([f64; 2], [f64; 2])> {
        if face == 0 {
            Some((*v0, *v1))
        } else {
            None
        }
    }

    #[inline]
    fn uv_to_st(u: f64) -> f64 {
        (u + 1.0) / 2.0
    }

    #[inline]
    fn st_to_uv(s: f64) -> f64 {
        2.0 * s - 1.0
    }

    #[inline]
    fn contains_point(point: &Self::Point, ring: &[Self::Point], _origin_inside: bool) -> bool {
        math::point_in_polygon(point, ring)
    }
}
