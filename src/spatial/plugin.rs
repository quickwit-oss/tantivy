//! Spatial fields as a [`SegmentPlugin`].
//!
//! A segment's spatial fields live in two composite files, the cell indexes and the edge
//! indexes, one section per field. The plugin writes them when a segment is flushed, merges them,
//! and reports their space usage. A segment of a schema with no spatial field has neither file.

use std::any::Any;
use std::collections::BTreeMap;
use std::io::Write;

use super::cell_index::{write_cell, write_directory};
use super::cell_index_reader::CellIndexReader;
use super::edge_cache::EdgeCache;
use super::edge_reader::EdgeReader;
use super::edge_writer::EdgeWriter;
use super::interleave::HeapInterleave;
use super::serializer::{SpatialSerializer, EDGE_SKIP_INTERVAL};
use super::sphere::Sphere;
use super::writer::SpatialWriter;
use crate::directory::CompositeWrite;
use crate::index::{SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::schema::document::{Document, Value};
use crate::schema::{FieldType, Schema};
use crate::space_usage::{ComponentSpaceUsage, SPATIAL_CELLS, SPATIAL_EDGES};
use crate::{DocId, Segment};

/// Extension of a segment's cell index file.
pub(crate) const CELLS_EXTENSION: &str = "cells";
/// Extension of a segment's edge index file.
pub(crate) const EDGES_EXTENSION: &str = "edges";

/// The component of a segment's cell indexes.
pub(crate) fn cells_component() -> SegmentComponent {
    SegmentComponent::Custom(CELLS_EXTENSION.to_string())
}

/// The component of a segment's edge indexes.
pub(crate) fn edges_component() -> SegmentComponent {
    SegmentComponent::Custom(EDGES_EXTENSION.to_string())
}

/// Bytes of geometry the edge cache of a spatial merge may hold before it evicts geometries:
/// enough for the geometries around the merge's position on the Hilbert curve, without holding a
/// segment's worth of vertex data in memory.
const MERGE_EDGE_CACHE_BYTES: usize = 64 << 20;

/// Built-in segment plugin that indexes, stores and merges spatial fields.
pub struct SpatialPlugin;

impl SegmentPlugin for SpatialPlugin {
    fn extensions(&self) -> &[&str] {
        &[CELLS_EXTENSION, EDGES_EXTENSION]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        Ok(Box::new(SpatialPluginWriter::new(ctx)))
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        merge_spatial_fields(&ctx)
    }

    fn space_usage(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BTreeMap<String, ComponentSpaceUsage>> {
        let spatial = segment_reader.spatial_fields();
        let schema = segment_reader.schema();
        Ok(BTreeMap::from([
            (
                SPATIAL_CELLS.to_string(),
                ComponentSpaceUsage::PerField(spatial.cells_space_usage(schema)),
            ),
            (
                SPATIAL_EDGES.to_string(),
                ComponentSpaceUsage::PerField(spatial.edges_space_usage(schema)),
            ),
        ]))
    }
}

/// Accumulates the geometry of a segment's spatial fields, and writes their indexes when the
/// segment is flushed.
pub struct SpatialPluginWriter {
    writer: SpatialWriter,
    has_spatial_field: bool,
}

impl SpatialPluginWriter {
    pub(crate) fn new(ctx: &PluginWriterContext) -> Self {
        SpatialPluginWriter {
            writer: SpatialWriter::default(),
            has_spatial_field: ctx.segment.schema().contains_spatial_field(),
        }
    }

    /// Generic, zero-copy document ingestion, called directly by the `SegmentWriter`.
    pub(crate) fn index_document<D: Document>(
        &mut self,
        doc_id: DocId,
        doc: &D,
        schema: &Schema,
    ) -> crate::Result<()> {
        if !self.has_spatial_field {
            return Ok(());
        }
        for (field, value) in doc.iter_fields_and_values() {
            let FieldType::Spatial(options) = schema.get_field_entry(field).field_type() else {
                continue;
            };
            if let Some(geometry) = value.as_value().into_geometry() {
                self.writer
                    .add_geometry(doc_id, field, *geometry, options.ring_orientation());
            }
        }
        Ok(())
    }
}

impl PluginWriter for SpatialPluginWriter {
    fn serialize(
        mut self: Box<Self>,
        segment: &Segment,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<()> {
        if !self.has_spatial_field {
            return Ok(());
        }
        if let Some(mapping) = doc_id_map {
            self.writer
                .remap_doc_ids(|doc_id| mapping.get_new_doc_id(doc_id));
        }
        let cells = segment.open_write(cells_component())?;
        let edges = segment.open_write(edges_component())?;
        self.writer
            .serialize(SpatialSerializer::new(cells, edges)?)?;
        Ok(())
    }

    fn mem_usage(&self) -> usize {
        self.writer.mem_usage()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Merge the spatial fields of the source segments into the target segment. Each field's cells
/// are interleaved in Hilbert order, and its geometry sets are renumbered in the order the cells
/// first reach them, under their documents' new ids.
fn merge_spatial_fields(ctx: &PluginMergeContext) -> crate::Result<()> {
    if !ctx.schema.contains_spatial_field() {
        return Ok(());
    }
    let target_segment = ctx.target_segment;
    let mut cells_composite = CompositeWrite::wrap(target_segment.open_write(cells_component())?);
    let mut edges_composite = CompositeWrite::wrap(target_segment.open_write(edges_component())?);

    // The spatial fields that have geometry in some segment, with every segment's reader.
    // Collected first so they live long enough for the readers they produce.
    let mut fields = Vec::new();
    for (field, field_entry) in ctx.schema.fields() {
        if !matches!(field_entry.field_type(), FieldType::Spatial(_)) {
            continue;
        }
        let spatial_readers = ctx
            .readers
            .iter()
            .map(|reader| reader.spatial_fields().get_field(field))
            .collect::<crate::Result<Vec<_>>>()?;
        if spatial_readers.iter().any(Option::is_some) {
            fields.push((field, spatial_readers));
        }
    }

    // Build inverted doc_id mapping: (segment_ord, old_doc_id) → new_doc_id. Only segments
    // with geometry get a row, since only their doc ids are looked up.
    let mut doc_id_inverse: Vec<Vec<Option<DocId>>> = Vec::new();
    if !fields.is_empty() {
        for (segment_ord, reader) in ctx.readers.iter().enumerate() {
            let has_geometry = fields
                .iter()
                .any(|(_, spatial_readers)| spatial_readers[segment_ord].is_some());
            doc_id_inverse.push(if has_geometry {
                vec![None; reader.max_doc() as usize]
            } else {
                Vec::new()
            });
        }
        for (new_doc_id, old_addr) in ctx.doc_id_mapping.iter_old_doc_addrs().enumerate() {
            let row = &mut doc_id_inverse[old_addr.segment_ord as usize];
            if let Some(slot) = row.get_mut(old_addr.doc_id as usize) {
                *slot = Some(new_doc_id as DocId);
            }
        }
    }
    for (field, spatial_readers) in &fields {
        // Open source cell index readers and edge readers. A segment without geometry for the
        // field gets empty readers, so that geometry ids keep their segment ordinals.
        let mut cell_readers: Vec<CellIndexReader> = Vec::new();
        let mut edge_readers: Vec<EdgeReader<'_, Sphere>> = Vec::new();
        let mut geometry_counts: Vec<u32> = Vec::new();

        for sr in spatial_readers {
            if let Some(spatial_reader) = sr {
                let cr = CellIndexReader::open(spatial_reader.cells_bytes())?;
                let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                geometry_counts.push(er.geometry_count());
                cell_readers.push(cr);
                edge_readers.push(er);
            } else {
                geometry_counts.push(0);
                cell_readers.push(CellIndexReader::empty());
                edge_readers.push(EdgeReader::<Sphere>::empty());
            }
        }

        // Like the segment writer, leave a field without geometry out of the composites.
        let total_source_geometries: u32 = geometry_counts.iter().sum();
        if total_source_geometries == 0 {
            continue;
        }

        let edge_cache = EdgeCache::new(edge_readers, MERGE_EDGE_CACHE_BYTES);
        let iters = cell_readers.iter().map(|cr| cr.iter()).collect();
        let mut interleave =
            HeapInterleave::new(iters, &edge_cache, &ctx.doc_id_mapping.alive_bitsets);

        const NOT_ASSIGNED: u32 = u32::MAX;
        let mut old_to_new: Vec<Vec<u32>> = geometry_counts
            .iter()
            .map(|&count| vec![NOT_ASSIGNED; count as usize])
            .collect();
        let mut next_new_id: u32 = 0;

        let merge_start = std::time::Instant::now();

        let edges_out = edges_composite.for_field(*field);
        let mut edge_writer = EdgeWriter::<Sphere>::new(edges_out, EDGE_SKIP_INTERVAL);
        let cells_out = cells_composite.for_field(*field);
        let cells_base = cells_out.written_bytes();
        let mut offsets: Vec<(u64, u64)> = Vec::new();
        let mut cell_count: u64 = 0;

        for cell in interleave.by_ref() {
            cell_count += 1;
            for shape in &cell.shapes {
                let segment = shape.geometry_id.0 as usize;
                let position = shape.geometry_id.1 as usize;
                if old_to_new[segment][position] != NOT_ASSIGNED {
                    continue;
                }
                let entry = edge_cache.get(shape.geometry_id);
                let set_size = entry.member_count();
                for i in 0..set_size {
                    let pos = (entry.head() + i) as usize;
                    if old_to_new[segment][pos] == NOT_ASSIGNED {
                        old_to_new[segment][pos] = next_new_id;
                        next_new_id += 1;
                    }
                }
                let old_doc_id = entry.doc_id();
                let new_doc_id = doc_id_inverse[segment][old_doc_id as usize].unwrap();
                edge_writer.insert_for_doc(entry.geometry_set(), new_doc_id)?;
            }

            offsets.push((cell.cell_id.0, cells_out.written_bytes() - cells_base));
            write_cell(cells_out, &cell, |shape| {
                let new_id = old_to_new[shape.geometry_id.0 as usize][shape.geometry_id.1 as usize];
                debug_assert_ne!(new_id, NOT_ASSIGNED);
                new_id
            })?;
        }

        edge_writer.finish()?;

        debug!(
            "spatial merge: {} source geometries, {} output geometries, {} cells, {:.2}s",
            total_source_geometries,
            next_new_id,
            cell_count,
            merge_start.elapsed().as_secs_f64()
        );
        interleave.report();

        // Cell index dictionary and footer.
        let dir_offset = cells_out.written_bytes() - cells_base;
        write_directory(cells_out, &offsets, dir_offset)?;
        cells_out.flush()?;
        edges_out.flush()?;
    }

    cells_composite.close()?;
    edges_composite.close()?;
    Ok(())
}
