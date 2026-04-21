//! Spatial field writer.
//!
//! Accumulates geometry per field during indexing. At segment flush, builds cell indexes and
//! streams edge data through the SpatialSerializer.
use std::collections::HashMap;
use std::io;

use std::marker::PhantomData;

use crate::schema::Field;
use crate::spatial::geometry::Geometry;
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::surface::Surface;
use crate::DocId;

/// Per-field accumulated documents with their geometries.
struct FieldData<S: Surface> {
    sets: Vec<GeometrySet<S>>,
}

impl<S: Surface> FieldData<S> {
    fn new() -> Self {
        Self { sets: Vec::new() }
    }
}

/// Accumulates spatial geometry during indexing, then serializes at segment flush.
pub struct SpatialWriter<S: Surface> {
    data_by_field: HashMap<Field, FieldData<S>>,
    _surface: PhantomData<S>,
}

impl<S: Surface> SpatialWriter<S> {
    /// Add a geometry for a document and field.
    pub fn add_geometry(&mut self, doc_id: DocId, field: Field, geometry: Geometry<Plane>) {
        let data = self
            .data_by_field
            .entry(field)
            .or_insert_with(FieldData::new);
        let projected = geometry.project::<S>();
        let set = to_geometry_set(&projected, doc_id);
        data.sets.push(set);
    }

    /// Memory usage estimate.
    pub fn mem_usage(&self) -> usize {
        self.data_by_field
            .values()
            .map(|data| {
                data.sets
                    .iter()
                    .map(|set| {
                        set.members
                            .iter()
                            .map(|m| m.vertices.len() * S::DIMENSIONS * 8)
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum()
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

impl<S: Surface> Default for SpatialWriter<S> {
    fn default() -> Self {
        SpatialWriter {
            data_by_field: HashMap::new(),
            _surface: PhantomData,
        }
    }
}
