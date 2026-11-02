use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::schema::document::{ErasedDocument, ErasedValue, ReferenceValueLeaf};
use crate::schema::{Field, FieldType, Schema};
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::spatial_index_manager::{
    MergeSource, SpatialFieldWriter, SpatialIndex, SurfaceIndex,
};
use crate::spatial::sphere::Sphere;
use crate::{DocId, TantivyError};

/// Empty spatial index catalog.
pub struct Nil;

/// A spatial index catalog entry followed by the rest of the catalog.
pub struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

/// Creates one concrete spatial index implementation.
pub trait SpatialIndexFactory: Send + Sync + 'static {
    /// The name used by a spatial field in the schema.
    const NAME: &'static str;

    /// Create the implementation.
    fn create() -> Box<dyn SpatialIndex>;
}

impl SpatialIndexFactory for SurfaceIndex<Sphere> {
    const NAME: &'static str = "sphere";

    fn create() -> Box<dyn SpatialIndex> {
        Box::new(SurfaceIndex::<Sphere>::new())
    }
}

impl SpatialIndexFactory for SurfaceIndex<Plane> {
    const NAME: &'static str = "plane";

    fn create() -> Box<dyn SpatialIndex> {
        Box::new(SurfaceIndex::<Plane>::new())
    }
}

/// A compile-time catalog of spatial index implementations.
pub trait SpatialIndexCatalog: Send + Sync + 'static {
    #[doc(hidden)]
    fn collect(name: &str, matches: &mut Vec<Box<dyn SpatialIndex>>);

    #[doc(hidden)]
    fn resolve(name: &str) -> crate::Result<Box<dyn SpatialIndex>> {
        let mut matches = Vec::new();
        Self::collect(name, &mut matches);
        match matches.len() {
            1 => Ok(matches.pop().unwrap()),
            0 => Err(TantivyError::InvalidArgument(format!(
                "spatial index '{name}' is not registered"
            ))),
            _ => Err(TantivyError::InvalidArgument(format!(
                "spatial index '{name}' is registered more than once"
            ))),
        }
    }
}

impl SpatialIndexCatalog for Nil {
    fn collect(_name: &str, _matches: &mut Vec<Box<dyn SpatialIndex>>) {}
}

impl<Head, Tail> SpatialIndexCatalog for Cons<Head, Tail>
where
    Head: SpatialIndexFactory,
    Tail: SpatialIndexCatalog,
{
    fn collect(name: &str, matches: &mut Vec<Box<dyn SpatialIndex>>) {
        if name == Head::NAME {
            matches.push(Head::create());
        }
        Tail::collect(name, matches);
    }
}

/// Spatial segment component using the implementations in `Catalog`.
pub struct S2Plugin<Catalog>(PhantomData<Catalog>);

impl<Catalog> S2Plugin<Catalog> {
    /// Create a spatial segment plugin.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Catalog> Default for S2Plugin<Catalog> {
    fn default() -> Self {
        Self::new()
    }
}

struct SpatialPluginWriter {
    writers: HashMap<Field, Box<dyn SpatialFieldWriter>>,
}

impl SpatialPluginWriter {
    fn new<Catalog: SpatialIndexCatalog>(schema: &Schema) -> crate::Result<Self> {
        let mut writers = HashMap::new();
        for (field, entry) in schema.fields() {
            let FieldType::Spatial(options) = entry.field_type() else {
                continue;
            };
            let index = Catalog::resolve(options.spatial_index_name())?;
            writers.insert(field, index.create_field_writer());
        }
        Ok(Self { writers })
    }
}

impl PluginWriter for SpatialPluginWriter {
    fn add_document(
        &mut self,
        doc_id: DocId,
        document: &dyn ErasedDocument,
        schema: &Schema,
    ) -> crate::Result<()> {
        for (field, value) in document.erased_fields() {
            if !matches!(
                schema.get_field_entry(field).field_type(),
                FieldType::Spatial(_)
            ) {
                continue;
            }
            let ErasedValue::Leaf(ReferenceValueLeaf::Geometry(geometry)) = value else {
                continue;
            };
            if let Some(writer) = self.writers.get_mut(&field) {
                writer.add_geometry(doc_id, &geometry);
            }
        }
        Ok(())
    }

    fn serialize(
        mut self: Box<Self>,
        segment: &crate::Segment,
        doc_id_map: Option<&crate::indexer::doc_id_mapping::DocIdMapping>,
    ) -> crate::Result<()> {
        let Some(mut serializer) = SpatialSerializer::for_segment(segment)? else {
            return Ok(());
        };
        for (field, writer) in self.writers.drain() {
            let (cells, edges, _doc_ids) = serializer.for_field(field);
            writer.serialize(
                cells,
                edges,
                doc_id_map.map(|mapping| mapping.old_to_new_ids()),
            )?;
        }
        serializer.close()?;
        Ok(())
    }

    fn mem_usage(&self) -> usize {
        self.writers.values().map(|writer| writer.mem_usage()).sum()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<Catalog: SpatialIndexCatalog> SegmentPlugin for S2Plugin<Catalog> {
    fn extensions(&self) -> &[&str] {
        &["cells", "edges", "docids"]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        Ok(Box::new(SpatialPluginWriter::new::<Catalog>(
            &ctx.segment.schema(),
        )?))
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        let Some(serializer) = SpatialSerializer::for_segment(ctx.target_segment)? else {
            return Ok(());
        };
        let (mut cells_composite, mut edges_composite, mut doc_ids_composite) =
            serializer.into_composite_writes();

        let mut doc_id_inverse = ctx
            .readers
            .iter()
            .map(|reader| vec![None; reader.max_doc() as usize])
            .collect::<Vec<_>>();
        for (new_doc_id, old_addr) in ctx.doc_id_mapping.iter_old_doc_addrs().enumerate() {
            doc_id_inverse[old_addr.segment_ord as usize][old_addr.doc_id as usize] =
                Some(new_doc_id as DocId);
        }

        for (field, entry) in ctx.schema.fields() {
            let FieldType::Spatial(options) = entry.field_type() else {
                continue;
            };
            let index = Catalog::resolve(options.spatial_index_name())?;

            let mut readers = Vec::new();
            for reader in ctx.readers {
                readers.push(reader.spatial_fields().get_field(field)?);
            }
            let sources = readers
                .iter()
                .enumerate()
                .map(|(ordinal, reader)| {
                    let (cells, edges) = match reader {
                        Some(reader) => (reader.cells_bytes(), reader.edges_bytes()),
                        None => (&[][..], &[][..]),
                    };
                    MergeSource {
                        cells_bytes: cells,
                        edges_bytes: edges,
                        segment_name: ctx.readers[ordinal].segment_id().uuid_string(),
                    }
                })
                .collect::<Vec<_>>();

            index.merge_field(
                &sources,
                &ctx.doc_id_mapping.alive_bitsets,
                cells_composite.for_field(field),
                edges_composite.for_field(field),
                doc_ids_composite.for_field(field),
                &doc_id_inverse,
            )?;
        }

        cells_composite.close()?;
        edges_composite.close()?;
        doc_ids_composite.close()?;
        Ok(())
    }
}
