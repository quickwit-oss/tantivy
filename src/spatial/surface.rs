//! Geometry abstraction for spatial indexing.
//!
//! An abstraction one of two surfaces for geographic indices. For planar geometry, points are 2D
//! coordinates on a single face. For spherical geometry, points are 3D unit vectors on the six
//! faces of a cube projection.

/// An abstraction one of two surfaces for geographic indices. For planar geometry, points are 2D
/// coordinates on a single face. For spherical geometry, points are 3D unit vectors on the six
/// faces of a cube projection.
pub trait Surface {
    /// The point type of the surface.
    type Point: Copy;

    /// Project a lon/lat coordinate onto this surface.
    fn project(lon: f64, lat: f64) -> Self::Point;

    /// Whether the interior of a closed ring contains the surface's reference origin.
    fn origin_inside(ring: &[Self::Point]) -> bool;
}
