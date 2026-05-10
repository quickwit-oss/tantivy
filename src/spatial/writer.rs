//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use crate::schema::Field;
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
    pub fn serialize(&mut self, mut serializer: SpatialSerializer) -> io::Result<()> {
        for (field, writer) in self.writers.drain() {
            let (cells_write, edges_write) = serializer.for_field(field);
            writer.serialize(cells_write, edges_write)?;
        }
        serializer.close()?;
        Ok(())
    }
}
