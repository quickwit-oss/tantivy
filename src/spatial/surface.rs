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
}
