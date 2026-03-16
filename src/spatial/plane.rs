//! Implementation of a planar surface.

use crate::spatial::surface::Surface;

/// Implementation of a planar surface.
#[derive(Debug, Clone, PartialEq)]
pub struct Plane;

impl Surface for Plane {
    type Point = [f64; 2];

    fn project(lon: f64, lat: f64) -> [f64; 2] {
        [lon, lat]
    }

    fn origin_inside(_ring: &[Self::Point]) -> bool {
        todo!("2D origin_inside")
    }
}
