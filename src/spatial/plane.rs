//! Implementation of a planar surface.

use crate::spatial::surface::Surface;

/// Implementation of a planar surface.
#[derive(Debug, Clone, PartialEq)]
pub struct Plane;

impl Surface for Plane {
    type Point = [f64; 2];
}
