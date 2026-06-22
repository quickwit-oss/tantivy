//! Per-segment row→doc_id map for vector fields.
//!
//! Stored as slot `[0]` of the `.vec` composite file, parallel to the dense
//! row blob in slot `[1]`. The variant is the storage-mode discriminator: the
//! flat backend writes `Identity` (dense) or `Bitmap` (sparse); the future IVF
//! backend writes `Explicit`. Reading the variant tag is all it takes to learn
//! the mode — there is no separate format byte or metadata file.
//!
//! For the flat variants the map also addresses the dense row array via rank
//! (`rank(doc_id) -> row_id`) and distinguishes "missing vector" from "zero
//! vector" at query time.
//!
//! Mirrors the `Full | Optional` cardinality split in `tantivy-columnar`. For
//! dense columns (every doc present — the typical case for embeddings) the
//! `Identity` variant skips the bitmap entirely: `row_id == doc_id` is the
//! identity map, no rank lookup needed. For sparse columns we delegate to
//! columnar's [`OptionalIndex`], a roaring-style bitmap with rank/select
//! support that's also used by fast-field columns elsewhere in tantivy.
//!
//! ## On-disk layout
//!
//! ```text
//! [u8 variant_tag] [body]
//!   tag = 0  (Identity): no body — `num_docs` comes from the caller
//!                        (typically `segment_reader.max_doc()`)
//!   tag = 1  (Bitmap):   body = serialized columnar OptionalIndex
//!   tag = 2  (Explicit): body = row→doc_id permutation, one u32 LE per row
//! ```
//!
//! `Identity`/`Bitmap` are written by the flat backend (`row_id == doc_id` or a
//! presence bitmap). `Explicit` is written by the IVF backend, where rows are
//! cluster-sorted and bear no positional relationship to `doc_id`; the variant
//! tag is therefore the flat-vs-IVF discriminator (`Explicit` ⟺ IVF ⟺ a
//! sibling `.centroids` file is present).

use std::io::{self, Write};
use std::mem::size_of;

use columnar::column_index::{open_optional_index, serialize_optional_index, OptionalIndex, Set};
use common::{BinarySerializable, HasLen, OwnedBytes};

use crate::directory::FileSlice;
use crate::DocId;

const VARIANT_IDENTITY: u8 = 0;
const VARIANT_BITMAP: u8 = 1;
pub(crate) const VARIANT_EXPLICIT: u8 = 2;

/// Decode the `row`-th doc_id from a packed little-endian `Explicit` body.
/// Caller guarantees `row < bytes.len() / 4`.
#[inline]
fn explicit_doc_id_at(bytes: &[u8], row: usize) -> DocId {
    let start = row * size_of::<DocId>();
    DocId::from_le_bytes(bytes[start..start + size_of::<DocId>()].try_into().unwrap())
}

/// Per-field row→doc_id map. Dispatches on cardinality at open time so the hot
/// path can skip the bitmap entirely when every doc has a value.
pub enum IdMap {
    /// Every doc has a value. `row_id == doc_id`; no bitmap stored.
    Identity { num_docs: u32 },
    /// Some docs may be absent. Rank/contains go through columnar's
    /// `OptionalIndex` (roaring-style block bitmap).
    Bitmap(OptionalIndex),
    /// IVF: maps each row to its doc_id. Held as the raw little-endian body
    /// (one u32 per row) so it can be decoded a row at a time.
    Explicit(OwnedBytes),
}

impl IdMap {
    /// Serialize the appropriate variant given a sorted list of present
    /// `doc_id`s. Chooses `Identity` if every doc is present, `Bitmap`
    /// otherwise.
    ///
    /// The Identity variant writes only the variant tag — `num_docs` is
    /// supplied at open time (typically from `segment_reader.max_doc()`).
    pub fn serialize<W: Write>(
        present_doc_ids: &[DocId],
        num_docs: u32,
        out: &mut W,
    ) -> io::Result<()> {
        if present_doc_ids.len() == num_docs as usize {
            out.write_all(&[VARIANT_IDENTITY])?;
        } else {
            out.write_all(&[VARIANT_BITMAP])?;
            serialize_optional_index(&present_doc_ids, num_docs, out)?;
        }
        Ok(())
    }

    pub fn serialize_explicit<W: Write>(row_doc_ids: &[DocId], out: &mut W) -> io::Result<()> {
        out.write_all(&[VARIANT_EXPLICIT])?;
        for doc_id in row_doc_ids {
            doc_id.serialize(out)?;
        }
        Ok(())
    }

    /// Parse a serialized id-map section, dispatching on the variant tag.
    /// `num_docs` is used only when the variant is `Identity` — for `Bitmap`,
    /// the count is read from the embedded `OptionalIndex` header.
    pub fn open(file_slice: FileSlice, num_docs: u32) -> io::Result<IdMap> {
        if file_slice.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "id map section is empty",
            ));
        }
        let tag = file_slice.slice(0..1).read_bytes()?[0];
        let body = file_slice.slice_from(1);
        match tag {
            VARIANT_IDENTITY => Ok(IdMap::Identity { num_docs }),
            VARIANT_BITMAP => Ok(IdMap::Bitmap(open_optional_index(body)?)),
            VARIANT_EXPLICIT => {
                let bytes = body.read_bytes()?;
                if bytes.len() % size_of::<DocId>() != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "explicit id map body is not a whole number of u32 doc ids",
                    ));
                }
                Ok(IdMap::Explicit(bytes))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown id map variant tag: {other}"),
            )),
        }
    }

    /// Number of docs that have a value (= number of stored rows).
    pub fn num_rows(&self) -> u32 {
        match self {
            IdMap::Identity { num_docs } => *num_docs,
            IdMap::Bitmap(idx) => idx.num_non_nulls(),
            IdMap::Explicit(bytes) => (bytes.len() / size_of::<DocId>()) as u32,
        }
    }

    /// `true` if `doc_id` has a value. The `Explicit` arm is a linear scan —
    /// the reference semantics the format tests exercise; the read path
    /// ([`VectorIndexReader`](crate::vector::VectorIndexReader)) uses
    /// cluster-local binary search instead.
    #[cfg(test)]
    #[inline]
    pub fn contains(&self, doc_id: DocId) -> bool {
        match self {
            IdMap::Identity { num_docs } => doc_id < *num_docs,
            IdMap::Bitmap(idx) => Set::contains(idx, doc_id),
            IdMap::Explicit(bytes) => {
                let num_rows = bytes.len() / size_of::<DocId>();
                (0..num_rows).any(|row| explicit_doc_id_at(bytes, row) == doc_id)
            }
        }
    }

    /// Returns the dense row id for `doc_id` if it has a value, else `None`.
    /// For `Identity`, this is the identity map — no bitmap consulted. For
    /// `Explicit` this is a linear scan; the IVF read path uses cluster-local
    /// binary search instead (see
    /// [`VectorIndexReader`](crate::vector::VectorIndexReader)).
    /// Callers must pass a `doc_id` within the segment (`doc_id < max_doc`);
    /// this is asserted in debug builds.
    #[inline]
    pub fn rank_if_exists(&self, doc_id: DocId) -> Option<u32> {
        match self {
            IdMap::Identity { num_docs } => {
                debug_assert!(doc_id < *num_docs, "doc_id {doc_id} >= num_docs {num_docs}");
                Some(doc_id)
            }
            IdMap::Bitmap(idx) => Set::rank_if_exists(idx, doc_id),
            IdMap::Explicit(bytes) => {
                let num_rows = bytes.len() / size_of::<DocId>();
                (0..num_rows)
                    .find(|&row| explicit_doc_id_at(bytes, row) == doc_id)
                    .map(|row| row as u32)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(present: &[DocId], num_docs: u32) -> IdMap {
        let mut buf = Vec::new();
        IdMap::serialize(present, num_docs, &mut buf).unwrap();
        IdMap::open(FileSlice::from(buf), num_docs).unwrap()
    }

    #[test]
    fn test_all_present_uses_identity_variant() {
        let n = 100u32;
        let present: Vec<DocId> = (0..n).collect();

        // Wire-level: the serialized output is exactly 1 byte (just the
        // variant tag); no body — num_docs comes from the caller.
        let mut buf = Vec::new();
        IdMap::serialize(&present, n, &mut buf).unwrap();
        assert_eq!(buf.len(), 1, "Identity variant should write only the tag");
        assert_eq!(buf[0], VARIANT_IDENTITY);

        let p = IdMap::open(FileSlice::from(buf), n).unwrap();
        assert!(matches!(p, IdMap::Identity { num_docs } if num_docs == n));
        assert_eq!(p.num_rows(), n);
        for d in 0..n {
            assert!(p.contains(d));
            assert_eq!(p.rank_if_exists(d), Some(d));
        }
        // Out-of-range queries are the caller's responsibility:
        // `contains` returns false, but `rank_if_exists` requires
        // `doc_id < num_docs` (asserted in debug builds).
        assert!(!p.contains(n));
    }

    #[test]
    fn test_none_present_uses_bitmap_variant() {
        let p = round_trip(&[], 100);
        assert!(matches!(p, IdMap::Bitmap(_)));
        assert_eq!(p.num_rows(), 0);
        for d in 0..100 {
            assert!(!p.contains(d));
            assert_eq!(p.rank_if_exists(d), None);
        }
    }

    #[test]
    fn test_sparse_uses_bitmap_variant() {
        let present: Vec<DocId> = vec![3, 7, 11, 12, 50, 99];
        let p = round_trip(&present, 100);
        assert!(matches!(p, IdMap::Bitmap(_)));
        assert_eq!(p.num_rows(), 6);
        for (row, &doc) in present.iter().enumerate() {
            assert!(p.contains(doc));
            assert_eq!(p.rank_if_exists(doc), Some(row as u32));
        }
        for d in [0u32, 1, 2, 4, 5, 6, 8, 9, 10, 13, 49, 51, 98] {
            assert!(!p.contains(d));
            assert_eq!(p.rank_if_exists(d), None);
        }
    }

    #[test]
    fn test_bitmap_across_blocks() {
        // Exercise multiple roaring-style blocks (each spans 64K docs).
        let n = 1500u32;
        let present: Vec<DocId> = (0..n).filter(|d| d % 3 == 0).collect();
        let p = round_trip(&present, n);
        assert!(matches!(p, IdMap::Bitmap(_)));
        assert_eq!(p.num_rows() as usize, present.len());
        for (row, &doc) in present.iter().enumerate() {
            assert_eq!(p.rank_if_exists(doc), Some(row as u32));
        }
        for d in 0..n {
            if d % 3 != 0 {
                assert!(!p.contains(d));
            }
        }
    }

    #[test]
    fn test_doc_id_beyond_num_docs() {
        let p = round_trip(&[1, 5], 10);
        assert!(!p.contains(10));
        assert!(!p.contains(100));
        assert_eq!(p.rank_if_exists(10), None);
    }

    #[test]
    fn test_explicit_round_trip() {
        // A cluster-sorted permutation: rows 0..2 are cluster 0 (docs 1,4),
        // rows 2..4 are cluster 1 (docs 0,3) — not globally sorted.
        let row_doc_ids: Vec<DocId> = vec![1, 4, 0, 3];
        let mut buf = Vec::new();
        IdMap::serialize_explicit(&row_doc_ids, &mut buf).unwrap();
        assert_eq!(buf[0], VARIANT_EXPLICIT);

        let p = IdMap::open(FileSlice::from(buf), 5).unwrap();
        assert!(matches!(p, IdMap::Explicit(_)));
        assert_eq!(p.num_rows(), 4);
        for (row, &doc) in row_doc_ids.iter().enumerate() {
            assert!(p.contains(doc));
            assert_eq!(p.rank_if_exists(doc), Some(row as u32));
        }
        assert!(!p.contains(2));
        assert_eq!(p.rank_if_exists(2), None);
    }
}
