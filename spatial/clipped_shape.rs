//! Geometry types shared by the cell index builder and the merge interleave.

/// Identifies a geometry member within a segment or across segments during merge. The first
/// element is the segment index (0 for single-segment operations). The second is the geometry
/// position within that segment's edge index.
pub type GeometryId = (u16, u32);

/// Represents the part of a shape that intersects an S2Cell.
///
/// It consists of the set of edge ids that intersect that cell, and a boolean indicating whether
/// the center of the cell is inside the shape (for shapes that have an interior).
///
/// Note that the edges themselves are not clipped; we always use the original edges for
/// intersection tests so that the results will be the same as the original shape.
#[derive(Clone, Debug, PartialEq)]
pub struct ClippedShape {
    /// Identifies the geometry member. (segment, position) during merge, (0, position) otherwise.
    pub geometry_id: GeometryId,
    /// True if the geometry contains the center of the cell.
    pub contains_center: bool,
    /// Indicies of the edges stored per geometry id in the edge index.
    pub edge_indices: Vec<u32>,
}

impl ClippedShape {
    /// Creates a new ClippedShape with the given geometry ID and contains_center flag.
    pub fn new(geometry_id: GeometryId, contains_center: bool) -> Self {
        Self {
            geometry_id,
            contains_center,
            edge_indices: Vec::new(),
        }
    }

    /// Adds an edge index to this clipped shape.
    pub fn add_edge(&mut self, edge_id: u32) {
        self.edge_indices.push(edge_id);
    }

    /// Returns the number of edges that intersect the S2CellId.
    #[inline]
    pub fn num_edges(&self) -> usize {
        self.edge_indices.len()
    }
}
