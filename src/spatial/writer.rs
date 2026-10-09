//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::sphere::Sphere;
use crate::DocId;

/// Per-field accumulated documents with their geometries.
#[derive(Default)]
struct FieldData {
    sets: Vec<GeometrySet>,
}

/// Accumulates spatial geometry during indexing, then serializes at segment flush.
#[derive(Default)]
pub struct SpatialWriter {
    data_by_field: HashMap<Field, FieldData>,
    /// The sum of `GeometrySet::mem_usage` over every accumulated set. The indexer asks for the
    /// memory usage after every document, so it is kept current wherever a set is added.
    sets_mem_usage: usize,
}

impl SpatialWriter {
    /// Add a geometry for a document and field.
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry<Plane>) {
        let data = self.data_by_field.entry(field).or_default();
        let projected = geometry.project::<Sphere>();
        let set = to_geometry_set(&projected, doc_id);
        self.sets_mem_usage += set.mem_usage();
        data.sets.push(set);
    }

    /// Memory usage estimate: the heap held by the sets, and the arrays that hold the sets
    /// themselves, spare capacity included.
    pub fn mem_usage(&self) -> usize {
        self.sets_mem_usage
            + self
                .data_by_field
                .values()
                .map(|data| data.sets.capacity() * std::mem::size_of::<GeometrySet>())
                .sum::<usize>()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The memory usage recomputed from every set, spare capacity included.
    fn summed_mem_usage(writer: &SpatialWriter) -> usize {
        writer
            .data_by_field
            .values()
            .map(|data| {
                data.sets.capacity() * std::mem::size_of::<GeometrySet>()
                    + data.sets.iter().map(GeometrySet::mem_usage).sum::<usize>()
            })
            .sum()
    }

    #[test]
    fn test_mem_usage_counts_each_geometry_once() {
        let mut writer = SpatialWriter::default();
        assert_eq!(writer.mem_usage(), 0);
        let (points, lines) = (Field::from_field_id(0), Field::from_field_id(1));
        for doc in 0..1000 {
            writer.add_geometry(doc, points, Geometry::Point([19.94, 50.06]));
            if doc % 3 == 0 {
                let line = vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]];
                writer.add_geometry(doc, lines, Geometry::LineString(line));
            }
            assert_eq!(writer.mem_usage(), summed_mem_usage(&writer));
        }
    }
}
