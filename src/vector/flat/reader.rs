//! Reader for the flat vector format.
//!
//! Opens the segment's `.flatvec` composite file (one presence section +
//! one dense row blob per vector field) and hands out per-field
//! [`FlatVectorColumn`] views for sequential scan.

use std::collections::BTreeMap;

use common::OwnedBytes;

use super::presence::Presence;
use crate::directory::CompositeFile;
use crate::index::SegmentComponent;
use crate::schema::{Field, FieldType, VectorOptions};
use crate::vector::reader::VectorColumnReader;
use crate::{DocId, SegmentReader, TantivyError};

pub struct FlatVecReader {
    composite: CompositeFile,
    field_options: BTreeMap<Field, VectorOptions>,
    max_doc: u32,
}

impl FlatVecReader {
    pub(crate) fn open(segment_reader: &SegmentReader) -> crate::Result<Self> {
        let schema = segment_reader.schema();
        let mut field_options = BTreeMap::new();
        for (field, entry) in schema.fields() {
            if let FieldType::Vector(opts) = entry.field_type() {
                field_options.insert(field, opts.clone());
            }
        }
        let file_slice =
            segment_reader.open_read(SegmentComponent::Custom(super::FLATVEC_EXT.to_string()))?;
        Ok(Self {
            composite: CompositeFile::open(&file_slice)?,
            field_options,
            max_doc: segment_reader.max_doc(),
        })
    }
}

impl VectorColumnReader for FlatVecReader {
    type Column = FlatVectorColumn;

    fn open_column(&self, field: Field) -> crate::Result<FlatVectorColumn> {
        let options = self.field_options.get(&field).cloned().ok_or_else(|| {
            TantivyError::InvalidArgument(format!("field {field:?} is not a vector field"))
        })?;
        let presence_slice = self.composite.open_read_with_idx(field, 0).ok_or_else(|| {
            TantivyError::InternalError(format!(
                "no flat vector data for vector field {field:?} in segment"
            ))
        })?;
        let rows_slice = self.composite.open_read_with_idx(field, 1).ok_or_else(|| {
            TantivyError::InternalError(format!(
                "no flat vector data for vector field {field:?} in segment"
            ))
        })?;
        let presence = Presence::open(presence_slice, self.max_doc)?;
        let row_bytes = rows_slice.read_bytes()?;
        Ok(FlatVectorColumn {
            presence,
            row_bytes,
            options,
        })
    }

    fn count(&self, field: Field) -> crate::Result<usize> {
        self.field_options.get(&field).ok_or_else(|| {
            TantivyError::InvalidArgument(format!("field {field:?} is not a vector field"))
        })?;
        let presence_slice = self.composite.open_read_with_idx(field, 0).ok_or_else(|| {
            TantivyError::InternalError(format!(
                "no flat vector data for vector field {field:?} in segment"
            ))
        })?;
        let presence = Presence::open(presence_slice, self.max_doc)?;
        Ok(presence.num_non_null() as usize)
    }

    fn dim(&self, field: Field) -> crate::Result<usize> {
        self.field_options
            .get(&field)
            .map(VectorOptions::dim)
            .ok_or_else(|| {
                TantivyError::InvalidArgument(format!("field {field:?} is not a vector field"))
            })
    }
}

/// A view over one vector field's data within a single segment.
///
/// Layout:
/// - `presence`: [`Presence::Full`] for dense columns (no bitmap stored, `row_id == doc_id`) or
///   [`Presence::Optional`] for sparse columns (rank-supporting bitmap).
/// - `row_bytes`: dense LE blob, exactly `presence.num_non_null()` rows of the schema dtype.
///
/// Lookup is `presence.rank_if_exists(doc) -> row_idx`, then
/// `&row_bytes[row_idx * bytes_per_vector ..]`. In the `Full` case the rank
/// step is the identity map — no bitmap consulted.
pub struct FlatVectorColumn {
    presence: Presence,
    row_bytes: OwnedBytes,
    options: VectorOptions,
}

impl FlatVectorColumn {
    pub fn dim(&self) -> usize {
        self.options.dim()
    }

    /// Number of docs that actually have a vector value.
    pub fn len(&self) -> usize {
        self.presence.num_non_null() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.presence.num_non_null() == 0
    }

    /// `true` if `doc_id` has a stored vector.
    #[inline]
    pub fn contains(&self, doc_id: DocId) -> bool {
        self.presence.contains(doc_id)
    }

    /// Borrow the raw little-endian bytes for a single document.
    ///
    /// Returns `None` if `doc_id` has no vector. The returned slice is a
    /// zero-copy borrow into the column's `OwnedBytes`.
    #[inline]
    pub fn vector_bytes_at(&self, doc_id: DocId) -> Option<&[u8]> {
        let row_id = self.presence.rank_if_exists(doc_id)? as usize;
        let stride = self.options.bytes_per_vector();
        let start = row_id * stride;
        let end = start + stride;
        if end > self.row_bytes.len() {
            return None;
        }
        Some(&self.row_bytes[start..end])
    }
}
