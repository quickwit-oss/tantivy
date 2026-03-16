//! Implementation of a unit sphere surface.

use crate::spatial::surface::Surface;

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
pub struct Sphere;

impl Surface for Sphere {
    type Point = [f64; 3];

    fn project(lon: f64, lat: f64) -> [f64; 3] {
        let lat = lat.to_radians();
        let lon = lon.to_radians();
        let cos_lat = lat.cos();
        [cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin()]
    }

    #[inline]
    fn origin_inside(ring: &[Self::Point]) -> bool {
        crate::spatial::containment::compute_origin_inside(ring)
    }
}
