//! In-memory edge provider for query polygons.

use super::clipped_shape::GeometryId;
use super::edge_provider::EdgeProvider;
use super::geometry_set::{EdgeSet, GeometrySet};
use super::surface::Surface;

/// In-memory edge provider wrapping a smashed GeometrySet. Resolves edge indices into the
/// smashed vertex arrays. Smashed vertices include the closure vertex.
pub struct QueryEdgeProvider<S: Surface> {
    pub(crate) set: GeometrySet<S>,
}

impl<S: Surface> QueryEdgeProvider<S> {
    /// Returns the EdgeSet for the given geometry member.
    pub fn get_edge_set(&self, geometry_id: GeometryId) -> &EdgeSet<S> {
        &self.set.members[geometry_id.1 as usize]
    }
}

impl<S: Surface> EdgeProvider for QueryEdgeProvider<S> {
    type Point = S::Point;
    fn get_edge(&self, geometry_id: GeometryId, edge_idx: u32) -> (S::Point, S::Point) {
        let vertices = &self.set.members[geometry_id.1 as usize].vertices;
        let i = edge_idx as usize;
        if vertices.len() == 1 {
            assert_eq!(i, 0, "a point has only its degenerate edge");
            (vertices[0], vertices[0])
        } else {
            (vertices[i], vertices[i + 1])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::geometry::Geometry;
    use crate::spatial::geometry_set::to_geometry_set;
    use crate::spatial::sphere::Sphere;

    #[test]
    fn test_point_members_have_degenerate_edges() {
        let points = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]];
        let provider = QueryEdgeProvider {
            set: to_geometry_set(&Geometry::<Sphere>::MultiPoint(points.to_vec()), 0),
        };
        assert_eq!(provider.get_edge((0, 0), 0), (points[0], points[0]));
        assert_eq!(provider.get_edge((0, 1), 0), (points[1], points[1]));
    }
}
