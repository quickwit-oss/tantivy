//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, EdgeSet, GeometrySet};
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::sphere::Sphere;
use crate::DocId;

/// Per-field accumulated documents with their geometries.
struct FieldData {
    sets: Vec<GeometrySet>,
}

impl FieldData {
    fn new() -> Self {
        Self { sets: Vec::new() }
    }
}

/// Accumulates spatial geometry during indexing, then serializes at segment flush.
pub struct SpatialWriter {
    data_by_field: HashMap<Field, FieldData>,
    /// Heap held by the accumulated sets. Kept as a running total because the indexer asks for
    /// it after every document; summing the sets on each call made indexing quadratic.
    mem_usage: usize,
}

/// Heap one geometry set holds: the set, its members and their vertex and ring arrays.
fn set_mem_usage(set: &GeometrySet) -> usize {
    std::mem::size_of::<GeometrySet>()
        + set.members.capacity() * std::mem::size_of::<EdgeSet>()
        + set
            .members
            .iter()
            .map(|m| {
                m.vertices.capacity() * std::mem::size_of::<[f64; 3]>()
                    + m.ring_offsets.capacity() * std::mem::size_of::<usize>()
            })
            .sum::<usize>()
}

impl SpatialWriter {
    /// Add a geometry for a document and field.
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry<Plane>) {
        let data = self
            .data_by_field
            .entry(field)
            .or_insert_with(FieldData::new);
        let projected = geometry.project::<Sphere>();
        let set = to_geometry_set(&projected, doc_id);
        self.mem_usage += set_mem_usage(&set);
        data.sets.push(set);
    }

    /// Memory usage estimate.
    pub fn mem_usage(&self) -> usize {
        self.mem_usage
    }

    /// Serialize all fields.
    pub fn serialize(&mut self, mut serializer: SpatialSerializer) -> io::Result<()> {
        for (field, data) in &self.data_by_field {
            serializer.serialize_field(*field, &data.sets)?;
        }
        serializer.close()?;
        Ok(())
    }
}

impl Default for SpatialWriter {
    fn default() -> Self {
        SpatialWriter {
            data_by_field: HashMap::new(),
            mem_usage: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_usage_counts_each_geometry_once() {
        let mut writer = SpatialWriter::default();
        let field = Field::from_field_id(0);
        assert_eq!(writer.mem_usage(), 0);
        writer.add_geometry(0, field, Geometry::Point([19.94, 50.06]));
        let one = writer.mem_usage();
        assert!(
            one >= std::mem::size_of::<GeometrySet>() + std::mem::size_of::<[f64; 3]>(),
            "a point costs at least its set and its vertex, not just the vertex: {one}"
        );
        for doc in 1..1000 {
            writer.add_geometry(doc, field, Geometry::Point([19.94, 50.06]));
        }
        assert_eq!(writer.mem_usage(), 1000 * one);
    }
}
