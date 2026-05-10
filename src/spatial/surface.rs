//! Geometry abstraction for spatial indexing.
//!
//! An abstraction one of two surfaces for geographic indices. For planar geometry, points are 2D
//! coordinates on a single face. For spherical geometry, points are 3D unit vectors on the six
//! faces of a cube projection.

use super::edge_crosser::EdgeCrosser;
use super::s2cell_id::S2CellId;
use super::s2coords::si_ti_to_st;

/// An abstraction one of two surfaces for geographic indices. For planar geometry, points are 2D
/// coordinates on a single face. For spherical geometry, points are 3D unit vectors on the six
/// faces of a cube projection.
pub trait Surface {
    /// Number of f64 coordinates per vertex. 2 for Plane, 3 for Sphere.
    const DIMENSIONS: usize;

    /// The point type of the surface.
    type Point: Copy + PartialEq + AsRef<[f64]> + Send + Sync;

    /// The edge crosser for this surface.
    type EdgeCrosser: EdgeCrosser<Point = Self::Point>;

    /// Project a lon/lat coordinate onto this surface.
    fn project(lon: f64, lat: f64) -> Self::Point;

    /// Whether the interior of a closed ring contains the surface's reference origin.
    fn origin_inside(ring: &[Self::Point]) -> bool;

    /// The Hilbert curve start point on this surface. Face 0, UV (-1, -1).
    fn hilbert_start() -> Self::Point;

    /// Correct winding order after projection. On the sphere, projection from lon/lat can
    /// invert winding so rings are checked and reversed if necessary. On the plane, winding
    /// is preserved from the source GeoJSON and this is a noop.
    fn normalize_ring(ring: &mut Vec<Self::Point>);

    /// Construct a point from DIMENSIONS * 8 little-endian bytes.
    fn point_from_le_bytes(bytes: &[u8]) -> Self::Point;

    /// Euclidean distance between two points on this surface.
    fn edge_length(a: &Self::Point, b: &Self::Point) -> f64;

    /// Cell padding for edge clipping. Compensates for face clipping error on the
    /// sphere. Zero on the plane where clip_to_face is exact.
    const CELL_PADDING: f64;

    /// Number of cube faces. 6 for Sphere, 1 for Plane.
    const FACE_COUNT: i32;

    /// Which cube face a point belongs to.
    fn get_face(point: &Self::Point) -> i32;

    /// Project a point to UV coordinates on a face.
    fn point_to_face_uv(face: i32, point: &Self::Point) -> (f64, f64);

    /// Convert face UV coordinates to a point on this surface.
    fn face_uv_to_point(face: i32, u: f64, v: f64) -> Self::Point;

    /// Clip an edge to a padded face. Returns UV endpoints or None if the edge
    /// does not intersect the face.
    fn clip_to_face(
        v0: &Self::Point,
        v1: &Self::Point,
        face: i32,
        padding: f64,
    ) -> Option<([f64; 2], [f64; 2])>;

    /// Map UV [-1, 1] to ST [0, 1]. Quadratic on the sphere for equal-area cells,
    /// linear on the plane.
    fn uv_to_st(u: f64) -> f64;

    /// Map ST [0, 1] to UV [-1, 1]. Inverse of uv_to_st.
    fn st_to_uv(s: f64) -> f64;

    /// Returns the leaf cell ID containing the given point.
    fn cell_id_from_point(point: &Self::Point) -> S2CellId;

    /// Returns the center of a cell as a point on this surface.
    fn cell_center(cell_id: S2CellId) -> Self::Point {
        let (face, si, ti) = cell_id.get_center_si_ti();
        let u = Self::st_to_uv(si_ti_to_st(si));
        let v = Self::st_to_uv(si_ti_to_st(ti));
        Self::face_uv_to_point(face, u, v)
    }

    /// Whether the point is inside the closed ring.
    fn contains_point(point: &Self::Point, ring: &[Self::Point], origin_inside: bool) -> bool;
}
