use crate::spatial::surface::Surface;

#[derive(Debug, Clone, PartialEq)]
pub struct Plane;

impl Surface for Plane {
    type Point = [f64; 2];
}
