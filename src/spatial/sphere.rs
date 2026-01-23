use crate::spatial::surface::Surface;

pub struct Sphere;

impl Surface for Sphere {
    type Point = [f64; 3];
}
