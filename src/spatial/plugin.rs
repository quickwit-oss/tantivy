use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::schema::document::{ErasedDocument, ErasedValue, ReferenceValueLeaf};
use crate::schema::{Field, FieldEntry, FieldType, Schema};
use crate::spatial::geometry_set::{to_geometry_set, GeometrySet};
use crate::spatial::plane::Plane;
use crate::spatial::serializer::SpatialSerializer;
use crate::spatial::spatial_index_manager::{
    MergeSource, SpatialFieldWriter, SpatialIndex, SurfaceIndex,
};
use crate::spatial::sphere::Sphere;
use crate::spatial::surface::Surface;
use crate::{DocId, TantivyError};

/// Empty spatial index catalog.
pub struct Nil;

/// A spatial index catalog entry followed by the rest of the catalog.
pub struct Cons<Head, Tail>(PhantomData<(Head, Tail)>);

/// Converts one document representation into spatial index input.
pub trait SpatialInput: Send + Sync + 'static {
    /// Claim a schema field and return the spatial index implementation it requests.
    fn index_name(entry: &FieldEntry) -> crate::Result<Option<&str>>;

    /// Convert one document value into the representation consumed by the spatial index.
    fn convert<S: Surface>(
        doc_id: DocId,
        value: ErasedValue<'_>,
    ) -> crate::Result<Option<GeometrySet<S>>>;
}

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

/// Creates one concrete spatial index implementation.
pub trait SpatialIndexFactory<Input>: Send + Sync + 'static {
    /// The name used by a spatial field in the schema.
    const NAME: &'static str;

    /// Create the implementation.
    fn create_index() -> Box<dyn SpatialIndex>;

    /// Create a field writer using the selected document input.
    fn create_field_writer() -> Box<dyn SpatialFieldWriter>;
}

impl<Input: SpatialInput> SpatialIndexFactory<Input> for SurfaceIndex<Sphere> {
    const NAME: &'static str = "sphere";

    fn create_index() -> Box<dyn SpatialIndex> {
        Box::new(SurfaceIndex::<Sphere>::new())
    }

    fn create_field_writer() -> Box<dyn SpatialFieldWriter> {
        SurfaceIndex::<Sphere>::create_field_writer::<Input>()
    }
}

impl<Input: SpatialInput> SpatialIndexFactory<Input> for SurfaceIndex<Plane> {
    const NAME: &'static str = "plane";

    fn create_index() -> Box<dyn SpatialIndex> {
        Box::new(SurfaceIndex::<Plane>::new())
    }

    fn create_field_writer() -> Box<dyn SpatialFieldWriter> {
        SurfaceIndex::<Plane>::create_field_writer::<Input>()
    }
}

/// A compile-time catalog of spatial index implementations.
pub trait SpatialIndexCatalog<Input>: Send + Sync + 'static {
    #[doc(hidden)]
    fn collect_indexes(name: &str, matches: &mut Vec<Box<dyn SpatialIndex>>);

    #[doc(hidden)]
    fn collect_writers(name: &str, matches: &mut Vec<Box<dyn SpatialFieldWriter>>);

    #[doc(hidden)]
    fn resolve_index(name: &str) -> crate::Result<Box<dyn SpatialIndex>> {
        let mut matches = Vec::new();
        Self::collect_indexes(name, &mut matches);
        resolve_match(name, matches)
    }

    #[doc(hidden)]
    fn resolve_writer(name: &str) -> crate::Result<Box<dyn SpatialFieldWriter>> {
        let mut matches = Vec::new();
        Self::collect_writers(name, &mut matches);
        resolve_match(name, matches)
    }
}

fn resolve_match<T>(name: &str, mut matches: Vec<T>) -> crate::Result<T> {
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

impl<Input> SpatialIndexCatalog<Input> for Nil
where Input: SpatialInput
{
    fn collect_indexes(_name: &str, _matches: &mut Vec<Box<dyn SpatialIndex>>) {}

    fn collect_writers(_name: &str, _matches: &mut Vec<Box<dyn SpatialFieldWriter>>) {}
}

impl<Input, Head, Tail> SpatialIndexCatalog<Input> for Cons<Head, Tail>
where
    Input: SpatialInput,
    Head: SpatialIndexFactory<Input>,
    Tail: SpatialIndexCatalog<Input>,
{
    fn collect_indexes(name: &str, matches: &mut Vec<Box<dyn SpatialIndex>>) {
        if name == <Head as SpatialIndexFactory<Input>>::NAME {
            matches.push(Head::create_index());
        }
        Tail::collect_indexes(name, matches);
    }

    fn collect_writers(name: &str, matches: &mut Vec<Box<dyn SpatialFieldWriter>>) {
        if name == <Head as SpatialIndexFactory<Input>>::NAME {
            matches.push(Head::create_field_writer());
        }
        Tail::collect_writers(name, matches);
    }
}

/// Spatial segment component using `Input` and the implementations in `Catalog`.
pub struct S2Plugin<Catalog, Input>(PhantomData<(Catalog, Input)>);

impl<Catalog, Input> S2Plugin<Catalog, Input> {
    /// Create a spatial segment plugin.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Catalog, Input> Default for S2Plugin<Catalog, Input> {
    fn default() -> Self {
        Self::new()
    }
}

struct SpatialPluginWriter {
    writers: HashMap<Field, Box<dyn SpatialFieldWriter>>,
}

impl SpatialPluginWriter {
    fn new<Catalog, Input>(schema: &Schema) -> crate::Result<Self>
    where
        Catalog: SpatialIndexCatalog<Input>,
        Input: SpatialInput,
    {
        let mut writers = HashMap::new();
        for (field, entry) in schema.fields() {
            let Some(index_name) = Input::index_name(entry)? else {
                continue;
            };
            writers.insert(field, Catalog::resolve_writer(index_name)?);
        }
        Ok(Self { writers })
    }
}

impl PluginWriter for SpatialPluginWriter {
    fn add_document(
        &mut self,
        doc_id: DocId,
        document: &dyn ErasedDocument,
        _schema: &Schema,
    ) -> crate::Result<()> {
        for (field, value) in document.erased_fields() {
            if let Some(writer) = self.writers.get_mut(&field) {
                writer.add_value(doc_id, value)?;
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

impl<Catalog, Input> SegmentPlugin for S2Plugin<Catalog, Input>
where
    Catalog: SpatialIndexCatalog<Input>,
    Input: SpatialInput,
{
    fn extensions(&self) -> &[&str] {
        &["cells", "edges", "docids"]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        Ok(Box::new(SpatialPluginWriter::new::<Catalog, Input>(
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
            let Some(index_name) = Input::index_name(entry)? else {
                continue;
            };
            let index = Catalog::resolve_index(index_name)?;

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
