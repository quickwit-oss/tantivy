//! Flat-format merge routine.
//!
//! The flat format is one of two storage modes the unified
//! [`VectorPlugin`](crate::vector::VectorPlugin) can produce per merge.
//! This module exposes the merge body — copying raw vector bytes from
//! source segments into a single `.vec` composite file — so the parent
//! plugin can call it after the threshold check.

use std::io::Write;

use super::id_map::IdMap;
use crate::directory::{CompositeWrite, Directory};
use crate::index::SegmentComponent;
use crate::plugin::PluginMergeContext;
use crate::schema::FieldType;
use crate::vector::header::write_header;
use crate::vector::VEC_EXT;
use crate::DocId;

/// Merge source vectors into the target segment's `.vec` file.
///
/// Caller (`VectorPlugin::merge`) has already verified that the target
/// segment's doc count is below the clustering threshold. This routine
/// is unconditional — it always writes the flat `.vec` when called.
pub(crate) fn merge_flat(ctx: &PluginMergeContext) -> crate::Result<()> {
    let has_vector_field = ctx
        .schema
        .fields()
        .any(|(_, entry)| matches!(entry.field_type(), FieldType::Vector(_)));
    if !has_vector_field {
        return Ok(());
    }
    if ctx.cancel.wants_cancel() {
        return Err(crate::TantivyError::Cancelled);
    }
    let path = ctx
        .target_segment
        .relative_path(SegmentComponent::Custom(VEC_EXT.to_string()));
    let mut write = ctx.target_segment.index().directory().open_write(&path)?;
    write_header(&mut write)?;
    let mut composite = CompositeWrite::wrap(write);

    // num_docs in the target segment = number of alive docs aggregated
    // by the doc_id_mapping. Each call to iter_old_doc_addrs yields
    // exactly num_new_doc_ids items.
    let num_target_docs: u32 = ctx.readers.iter().map(|r| r.num_docs()).sum::<u32>();

    for (field, entry) in ctx.schema.fields() {
        let _opts = match entry.field_type() {
            FieldType::Vector(opts) => opts,
            _ => continue,
        };

        // Per-segment readers for this field (cached on the SegmentReaders).
        let field_readers: Vec<_> = ctx
            .readers
            .iter()
            .map(|reader| reader.vector_index(field))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut target_present: Vec<DocId> = Vec::new();
        let mut new_doc_id: DocId = 0;
        {
            let rows_w = composite.for_field_with_idx(field, 1);
            for old_doc_addr in ctx.doc_id_mapping.iter_old_doc_addrs() {
                let reader = &field_readers[old_doc_addr.segment_ord as usize];
                if let Some(bytes) = reader.vector_bytes(old_doc_addr.doc_id)? {
                    target_present.push(new_doc_id);
                    rows_w.write_all(&bytes)?;
                }
                new_doc_id += 1;
            }
            rows_w.flush()?;
        }

        // Sanity: the mapping iterator should yield exactly num_target_docs items.
        debug_assert_eq!(new_doc_id, num_target_docs);

        // Slice (field, 0): row→doc_id map (Identity or Bitmap).
        let id_map_w = composite.for_field_with_idx(field, 0);
        IdMap::serialize(&target_present, num_target_docs, id_map_w)?;
        id_map_w.flush()?;
    }
    composite.close()?;
    Ok(())
}
