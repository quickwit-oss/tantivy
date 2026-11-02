//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::document::{ErasedDocument, ErasedValue, ReferenceValueLeaf};
use crate::schema::{Field, FieldType, Schema};
use crate::spatial::geometry::Geometry;
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::spatial_index_manager::{SpatialFieldWriter, SpatialIndexManager};
use crate::DocId;

/// Accumulates spatial geometry during indexing, then serializes at segment flush.
pub struct SpatialWriter {
    writers: HashMap<Field, Box<dyn SpatialFieldWriter>>,
    manager: SpatialIndexManager,
    field_index_names: HashMap<Field, String>,
}

impl SpatialWriter {
    /// Create a new spatial writer backed by the given manager.
    pub fn new(manager: SpatialIndexManager) -> Self {
        SpatialWriter {
            writers: HashMap::new(),
            manager,
            field_index_names: HashMap::new(),
        }
    }

    /// Register the spatial index name for a field. Called during schema setup.
    pub fn set_field_index(&mut self, field: Field, name: &str) {
        self.field_index_names.insert(field, name.to_string());
    }

    /// Add the document's spatial geometry values.
    pub fn add_document(&mut self, doc_id: DocId, document: &dyn ErasedDocument, schema: &Schema) {
        if !schema.contains_spatial_field() {
            return;
        }
        for (field, value) in document.erased_fields() {
            if matches!(
                schema.get_field_entry(field).field_type(),
                FieldType::Spatial(_)
            ) {
                if let ErasedValue::Leaf(ReferenceValueLeaf::Geometry(geometry)) = value {
                    self.add_geometry(doc_id, field, *geometry);
                }
            }
        }
    }

    /// Add a geometry for a document and field.
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry<Plane>) {
        let writer = self.writers.entry(field).or_insert_with(|| {
            let name = self
                .field_index_names
                .get(&field)
                .map(|s| s.as_str())
                .unwrap_or("sphere");
            self.manager
                .get(name)
                .unwrap_or_else(|| panic!("spatial index '{}' not registered", name))
                .create_field_writer()
        });
        writer.add_geometry(doc_id, &geometry);
    }

    /// Memory usage estimate.
    pub fn mem_usage(&self) -> usize {
        self.writers.values().map(|w| w.mem_usage()).sum()
    }

    /// Serialize all fields.
    pub fn serialize(
        &mut self,
        mut serializer: SpatialSerializer,
        old_to_new: Option<&[DocId]>,
    ) -> io::Result<()> {
        for (field, writer) in self.writers.drain() {
            let (cells_write, edges_write, _doc_ids_write) = serializer.for_field(field);
            writer.serialize(cells_write, edges_write, old_to_new)?;
        }
        serializer.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use common::{CountingWriter, TerminatingWrite};

    use super::*;
    use crate::directory::{Directory, RamDirectory};
    use crate::schema::{OwnedValue, PLANE, TEXT};
    use crate::spatial::edge_reader::EdgeReader;
    use crate::TantivyDocument;

    #[test]
    fn test_erased_document_keeps_spatial_values_and_owner() {
        let mut builder = Schema::builder();
        let geometry = builder.add_spatial_field("geometry", PLANE);
        let other = builder.add_text_field("other", TEXT);
        let schema = builder.build();
        let point = OwnedValue::Geometry(Geometry::Point([10.0, 20.0]));
        let mut document = TantivyDocument::default();
        document.add_field_value(geometry, &point);
        document.add_field_value(geometry, &OwnedValue::U64(99));
        document.add_field_value(geometry, &OwnedValue::Array(vec![point.clone()]));
        document.add_field_value(
            geometry,
            &OwnedValue::Geometry(Geometry::MultiPoint(vec![[30.0, 40.0], [50.0, 60.0]])),
        );
        document.add_field_value(other, &point);

        let mut writer = SpatialWriter::new(SpatialIndexManager::default());
        writer.set_field_index(geometry, "plane");
        writer.add_document(7, &document, &schema);
        assert_eq!(writer.writers.len(), 1);

        let directory = RamDirectory::create();
        let mut cells = CountingWriter::wrap(directory.open_write(Path::new("cells")).unwrap());
        let mut edges = CountingWriter::wrap(directory.open_write(Path::new("edges")).unwrap());
        writer
            .writers
            .remove(&geometry)
            .unwrap()
            .serialize(&mut cells, &mut edges, None)
            .unwrap();
        cells.terminate().unwrap();
        edges.terminate().unwrap();
        let bytes = directory.atomic_read(Path::new("edges")).unwrap();
        let reader = EdgeReader::<Plane>::open(&bytes).unwrap();
        assert_eq!(reader.geometry_count(), 3);
        assert_eq!(reader.value_heads(6).unwrap().count(), 0);
        assert_eq!(
            reader.value_heads(7).unwrap().collect::<Vec<_>>(),
            vec![0, 1]
        );
        let (_, first) = reader.read_geometry_set(0);
        assert_eq!(first.doc_id, 7);
        assert_eq!(first.members[0].vertices, vec![[10.0, 20.0]]);
        let (_, second) = reader.read_geometry_set(1);
        assert_eq!(second.doc_id, 7);
        assert_eq!(second.members.len(), 2);
        assert_eq!(second.members[0].vertices, vec![[30.0, 40.0]]);
        assert_eq!(second.members[1].vertices, vec![[50.0, 60.0]]);
    }
}
