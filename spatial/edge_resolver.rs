//! Direct geometry access over one or more edge readers.

use super::clipped_shape::GeometryId;
use super::edge_reader::{EdgeReader, LocatedEntry};
use super::geometry_set::GeometrySet;
use super::surface::Surface;

/// Resolves segment-qualified geometry IDs against mapped edge data.
pub struct EdgeResolver<'a, S: Surface> {
    readers: Vec<EdgeReader<'a, S>>,
}

impl<'a, S: Surface> EdgeResolver<'a, S> {
    /// Create a resolver over the given segment readers.
    pub fn new(readers: Vec<EdgeReader<'a, S>>) -> Self {
        EdgeResolver { readers }
    }

    /// Total number of geometry members in the given segment.
    pub fn geometry_count(&self, segment: usize) -> u32 {
        self.readers[segment].geometry_count()
    }

    /// Resolve a geometry member to its document ID.
    pub fn doc_id_for(&self, id: GeometryId) -> u32 {
        self.readers[id.0 as usize].doc_id_for(id.1)
    }

    /// Locate a geometry member without decoding its coordinates.
    pub fn locate(&self, id: GeometryId) -> LocatedEntry<'a, S> {
        self.readers[id.0 as usize].locate_entry(id.1)
    }

    /// Decode the complete value containing a geometry member.
    pub fn read_geometry_set(&self, id: GeometryId) -> (u32, GeometrySet<S>) {
        self.readers[id.0 as usize].read_geometry_set(id.1)
    }
}
