use crate::schema::document::{ErasedValue, ReferenceValueLeaf};
use crate::schema::{FieldEntry, FieldType};
use crate::spatial::geometry::to_geometry_set;
use crate::spatial::geometry_set::GeometrySet;
use crate::spatial::plugin::SpatialInput;
use crate::spatial::surface::Surface;
use crate::DocId;

/// Native `SPATIAL` field input.
pub struct NativeSpatial;

impl SpatialInput for NativeSpatial {
    fn index_name(entry: &FieldEntry) -> crate::Result<Option<&str>> {
        let FieldType::Spatial(options) = entry.field_type() else {
            return Ok(None);
        };
        Ok(Some(options.spatial_index_name()))
    }

    fn convert<S: Surface>(
        doc_id: DocId,
        value: ErasedValue<'_>,
    ) -> crate::Result<Option<GeometrySet<S>>> {
        let ErasedValue::Leaf(ReferenceValueLeaf::Geometry(geometry)) = value else {
            return Ok(None);
        };
        let projected = geometry.project::<S>();
        Ok(Some(to_geometry_set(&projected, doc_id)))
    }
}
