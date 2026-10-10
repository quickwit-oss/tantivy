//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{
    to_geometry_set_with_orientation, EdgeSet, GeometrySet, RingOrientation,
};
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
    /// Add a geometry for a document and field, with polygon rings oriented as the field's are.
    /// The values of a document's field make up one geometry set, so that a query matches the
    /// document's geometry as a whole: contained only if all of it is.
    pub fn add_geometry(
        &mut self,
        doc_id: DocId,
        field: Field,
        geometry: Geometry<Plane>,
        orientation: RingOrientation,
    ) {
        let data = self.data_by_field.entry(field).or_default();
        let projected = geometry.project::<Sphere>();
        let set = to_geometry_set_with_orientation(&projected, doc_id, orientation);
        // An empty geometry, such as a polygon with no rings, adds nothing.
        if set.members.is_empty() {
            return;
        }
        match data.sets.last_mut() {
            Some(last) if last.doc_id == doc_id => {
                let member_size = std::mem::size_of::<EdgeSet>();
                // The incoming members keep their vertex and ring buffers. Only their outer
                // buffer is discarded; account for the destination buffer's growth separately.
                let added_buffers = set.mem_usage() - set.members.capacity() * member_size;
                let previous_capacity = last.members.capacity();
                last.members.extend(set.members);
                self.sets_mem_usage +=
                    added_buffers + (last.members.capacity() - previous_capacity) * member_size;
            }
            _ => {
                self.sets_mem_usage += set.mem_usage();
                data.sets.push(set);
            }
        }
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
    fn test_values_of_a_document_make_one_set() {
        let mut writer = SpatialWriter::default();
        let field = Field::from_field_id(0);
        writer.add_geometry(
            0,
            field,
            Geometry::Point([1.0, 1.0]),
            RingOrientation::Smaller,
        );
        writer.add_geometry(
            1,
            field,
            Geometry::Point([2.0, 2.0]),
            RingOrientation::Smaller,
        );
        writer.add_geometry(
            1,
            field,
            Geometry::MultiPoint(vec![[3.0, 3.0], [4.0, 4.0]]),
            RingOrientation::Smaller,
        );
        let sets = &writer.data_by_field[&field].sets;
        let members: Vec<(u32, usize)> = sets.iter().map(|s| (s.doc_id, s.members.len())).collect();
        assert_eq!(members, [(0, 1), (1, 3)]);
        assert_eq!(writer.mem_usage(), summed_mem_usage(&writer));
    }

    #[test]
    fn test_mem_usage_counts_each_geometry_once() {
        let mut writer = SpatialWriter::default();
        assert_eq!(writer.mem_usage(), 0);
        let (points, lines) = (Field::from_field_id(0), Field::from_field_id(1));
        for doc in 0..1000 {
            writer.add_geometry(
                doc,
                points,
                Geometry::Point([19.94, 50.06]),
                RingOrientation::Smaller,
            );
            if doc % 3 == 0 {
                let line = vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]];
                writer.add_geometry(
                    doc,
                    lines,
                    Geometry::LineString(line),
                    RingOrientation::Smaller,
                );
            }
            assert_eq!(writer.mem_usage(), summed_mem_usage(&writer));
        }
    }

    #[test]
    fn test_mem_usage_for_multiple_values_includes_spare_capacity() {
        let mut writer = SpatialWriter::default();
        let field = Field::from_field_id(0);
        for doc in 0..2 {
            for value in 0..257 {
                writer.add_geometry(
                    doc,
                    field,
                    Geometry::Point([value as f64 % 180.0, 1.0]),
                    RingOrientation::Smaller,
                );
                assert_eq!(writer.mem_usage(), summed_mem_usage(&writer));
            }
        }
        let sets = &writer.data_by_field[&field].sets;
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[1].members.len(), 257);
        assert!(sets[1].members.capacity() > sets[1].members.len());
    }
}
