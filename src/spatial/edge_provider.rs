//! Edge vertex resolution for spatial queries.

use super::clipped_shape::GeometryId;

/// Resolves a (geometry_id, edge_index) pair to the two endpoints of that edge.
pub trait EdgeProvider {
    /// The point type for edge vertices.
    type Point: Copy;
    /// Returns the two endpoints of the given edge.
    fn get_edge(&self, geometry_id: GeometryId, edge_index: u32) -> (Self::Point, Self::Point);
}
