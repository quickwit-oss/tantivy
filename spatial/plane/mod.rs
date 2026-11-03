//! Planar surface implementation and predicates for 2D spatial indexing.

mod contact;
pub mod crosser;
mod edge_index;
pub mod math;
mod operand;
pub(crate) mod query;
mod relation;
pub(crate) use relation::Predicate;

use crate::spatial::r1interval::R1Interval;
use crate::spatial::r2rect::R2Rect;
use crate::spatial::s2cell_id::S2CellId;
use crate::spatial::s2coords::{ij_to_st_min, st_to_ij, LIMIT_IJ};
use crate::spatial::surface::Surface;

/// Implementation of a planar surface.
#[derive(Debug, Clone, PartialEq)]
pub struct Plane;

#[inline]
fn source_to_ij(value: f64, scale: f64) -> i32 {
    let mut i = st_to_ij(Plane::uv_to_st(value / scale));
    // Grid dividers have at most 31 significant bits; multiplying by 90 or
    // 180 is exact. Correct the rounded estimate against those source dividers.
    while i > 0 && value < scale * Plane::st_to_uv(ij_to_st_min(i)) {
        i -= 1;
    }
    while i < LIMIT_IJ - 1 && value >= scale * Plane::st_to_uv(ij_to_st_min(i + 1)) {
        i += 1;
    }
    i
}

pub(crate) fn source_bound(bound: &R2Rect) -> R2Rect {
    R2Rect::new(
        R1Interval::new(180.0 * bound[0].lo(), 180.0 * bound[0].hi()),
        R1Interval::new(90.0 * bound[1].lo(), 90.0 * bound[1].hi()),
    )
}

impl Surface for Plane {
    const DIMENSIONS: usize = 2;
    type Point = [f64; 2];
    type EdgeCrosser = crosser::PlaneEdgeCrosser;

    fn project(lon: f64, lat: f64) -> [f64; 2] {
        [lon, lat]
    }

    fn hilbert_start() -> Self::Point {
        [-180.0, -90.0]
    }

    #[inline]
    fn edge_length(a: &Self::Point, b: &Self::Point) -> f64 {
        math::edge_length(&[a[0] / 180.0, a[1] / 90.0], &[b[0] / 180.0, b[1] / 90.0])
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

    fn origin_inside_source<F>(_len: usize, _vertex: F) -> bool
    where F: Fn(usize) -> Self::Point {
        false
    }

    const CELL_PADDING: f64 = 0.0;

    const EXACT_SUPPORT: bool = true;

    const RING_ROLES: bool = true;

    fn edge_bound_uv(v0: &Self::Point, v1: &Self::Point, _face: i32) -> R2Rect {
        let interval = |axis: usize, scale: f64| {
            R1Interval::new(
                (v0[axis].min(v1[axis]) / scale).next_down(),
                (v0[axis].max(v1[axis]) / scale).next_up(),
            )
        };
        R2Rect::new(interval(0, 180.0), interval(1, 90.0))
    }

    #[inline]
    fn edge_intersects_rect(v0: &Self::Point, v1: &Self::Point, face: i32, bound: &R2Rect) -> bool {
        face == 0 && math::segment_intersects_rect(v0, v1, &source_bound(bound))
    }

    const FACE_COUNT: i32 = 1;

    #[inline]
    fn get_face(_point: &Self::Point) -> i32 {
        0
    }

    #[inline]
    fn point_to_face_uv(_face: i32, point: &Self::Point) -> (f64, f64) {
        (point[0] / 180.0, point[1] / 90.0)
    }

    #[inline]
    fn face_uv_to_point(_face: i32, u: f64, v: f64) -> Self::Point {
        [180.0 * u, 90.0 * v]
    }

    #[inline]
    fn clip_to_face(
        v0: &Self::Point,
        v1: &Self::Point,
        face: i32,
        _padding: f64,
    ) -> Option<([f64; 2], [f64; 2])> {
        if face == 0 {
            Some(([v0[0] / 180.0, v0[1] / 90.0], [v1[0] / 180.0, v1[1] / 90.0]))
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
    fn cell_id_from_point(point: &Self::Point) -> S2CellId {
        let i = source_to_ij(point[0], 180.0);
        let j = source_to_ij(point[1], 90.0);
        S2CellId::from_face_ij(0, i, j)
    }

    #[inline]
    fn contains_point(point: &Self::Point, ring: &[Self::Point], _origin_inside: bool) -> bool {
        math::point_in_polygon(point, ring)
    }

    fn contains_point_source<F>(
        point: &Self::Point,
        len: usize,
        vertex: F,
        _origin_inside: bool,
    ) -> bool
    where
        F: Fn(usize) -> Self::Point,
    {
        math::point_in_polygon_source(point, len, vertex)
    }
}

#[cfg(test)]
#[path = "../tests/plane_cell_tests.rs"]
mod tests;
