use std::any::Any;
use std::collections::BTreeMap;
use std::io::Write;

use common::TerminatingWrite;

use super::presence::Presence;
use crate::directory::CompositeWrite;
use crate::index::{Segment, SegmentComponent};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::plugin::PluginWriter;
use crate::schema::document::{TantivyDocument, Value};
use crate::schema::{Field, FieldType, Schema};
use crate::vector::meta::{VectorStorageFormat, VECMETA_EXT};
use crate::{DocId, TantivyError};

/// Per-field in-memory state: the doc ids that have a value (ascending),
/// plus a dense byte array — analogous to fast-fields' `Optional`
/// cardinality. Docs without a vector occupy zero bytes of storage.
///
/// Rows are kept as raw little-endian bytes rather than decoded `T`
/// values. The bytes go in via `add_document` and come back out at
/// serialize time unchanged — no decode/re-encode round-trip, no
/// dependency on `T: VectorElement` in the writer.
struct FieldBuffer {
    /// Doc ids that have a value for this field, in insertion order
    /// (which equals ascending old-doc-id order since `add_document<D>`
    /// is called sequentially).
    present_doc_ids: Vec<DocId>,
    /// Dense byte blob: `row_bytes[i*stride..(i+1)*stride]` is the
    /// vector for `present_doc_ids[i]`.
    row_bytes: Vec<u8>,
    /// Bytes per vector (`dim * dtype.size_bytes()`).
    stride: usize,
}

impl FieldBuffer {
    fn push_bytes(&mut self, doc_id: DocId, bytes: &[u8]) {
        debug_assert_eq!(bytes.len(), self.stride);
        self.present_doc_ids.push(doc_id);
        self.row_bytes.extend_from_slice(bytes);
    }

    fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.present_doc_ids.capacity() * std::mem::size_of::<DocId>()
            + self.row_bytes.capacity()
    }
}

pub struct FlatVecWriter {
    fields: BTreeMap<Field, FieldBuffer>,
    /// Set by [`SegmentWriter::finalize`] before [`serialize`]. Used to
    /// size the presence bitmap.
    num_docs: DocId,
}

impl FlatVecWriter {
    pub fn for_schema(schema: &Schema) -> Self {
        let mut fields = BTreeMap::new();
        for (field, entry) in schema.fields() {
            if let FieldType::Vector(opts) = entry.field_type() {
                fields.insert(
                    field,
                    FieldBuffer {
                        present_doc_ids: Vec::new(),
                        row_bytes: Vec::new(),
                        stride: opts.bytes_per_vector(),
                    },
                );
            }
        }
        Self {
            fields,
            num_docs: 0,
        }
    }
}

impl PluginWriter for FlatVecWriter {
    fn add_document(
        &mut self,
        doc_id: DocId,
        doc: &TantivyDocument,
        schema: &Schema,
    ) -> crate::Result<()> {
        if self.fields.is_empty() {
            return Ok(());
        }
        self.num_docs = doc_id + 1;
        for (field, buf) in self.fields.iter_mut() {
            let value = match doc.get_first(*field) {
                Some(value) => value,
                None => continue,
            };
            let bytes = value.as_value().as_bytes().ok_or_else(|| {
                TantivyError::SchemaError(format!(
                    "Expected vector bytes for field {:?}",
                    schema.get_field_entry(*field).name()
                ))
            })?;
            if bytes.len() != buf.stride {
                return Err(TantivyError::SchemaError(format!(
                    "vector byte length mismatch for field {:?}: expected {} bytes, got {}",
                    schema.get_field_entry(*field).name(),
                    buf.stride,
                    bytes.len(),
                )));
            }
            buf.push_bytes(doc_id, bytes);
        }
        Ok(())
    }
    fn serialize(
        &mut self,
        segment: &Segment,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<()> {
        if self.fields.is_empty() {
            return Ok(());
        }
        let mut meta_write =
            segment.open_write(SegmentComponent::Custom(VECMETA_EXT.to_string()))?;
        VectorStorageFormat::Flat.serialize(&mut meta_write)?;
        meta_write.terminate()?;

        let write = segment.open_write(SegmentComponent::Custom(super::FLATVEC_EXT.to_string()))?;
        let mut composite = CompositeWrite::wrap(write);

        for (field, buf) in &self.fields {
            // Compute (present, row_bytes) in target doc-id order. For
            // the no-remap case the writer already accumulates in
            // ascending insertion (= target) order.
            let (present, row_bytes): (Vec<DocId>, Vec<u8>) = if let Some(map) = doc_id_map {
                let mut p = Vec::new();
                let mut r = Vec::new();
                for new_doc_id in 0..map.num_new_doc_ids() as DocId {
                    let old_doc_id = map.get_old_doc_id(new_doc_id);
                    if let Ok(row_idx) = buf.present_doc_ids.binary_search(&old_doc_id) {
                        p.push(new_doc_id);
                        let start = row_idx * buf.stride;
                        r.extend_from_slice(&buf.row_bytes[start..start + buf.stride]);
                    }
                }
                (p, r)
            } else {
                (buf.present_doc_ids.clone(), buf.row_bytes.clone())
            };

            // Slice (field, 0): presence section. Picks Full if every
            // doc is present (typical for dense embeddings, just one
            // tag byte) or Optional otherwise.
            let bitmap_w = composite.for_field_with_idx(*field, 0);
            Presence::serialize(&present, self.num_docs, bitmap_w)?;
            bitmap_w.flush()?;

            // Slice (field, 1): dense LE byte rows, one per present doc.
            let rows_w = composite.for_field_with_idx(*field, 1);
            rows_w.write_all(&row_bytes)?;
            rows_w.flush()?;
        }
        composite.close()?;
        Ok(())
    }

    fn close(self: Box<Self>) -> crate::Result<()> {
        Ok(())
    }

    fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .fields
                .values()
                .map(FieldBuffer::mem_usage)
                .sum::<usize>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
