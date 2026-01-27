//! Implementation of a unit sphere surface.

use crate::spatial::surface::Surface;

/// Implementation of a unit sphere surface.
pub struct Sphere;

impl Surface for Sphere {
    type Point = [f64; 3];
}
