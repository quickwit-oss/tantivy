//! Per-segment presence tracker for vector fields.
//!
//! Marks which `doc_id`s have a value for a given vector field. Used to
//! address the dense row array via rank (`rank(doc_id) -> row_id`) and
//! to distinguish "missing vector" from "zero vector" at query time.
//!
//! Mirrors the `Full | Optional` cardinality split in
//! `tantivy-columnar`. For dense columns (every doc present — the
//! typical case for embeddings) the `Full` variant skips the bitmap
//! entirely: `row_id == doc_id` is the identity map, no rank lookup
//! needed. For sparse columns we delegate to columnar's
//! [`OptionalIndex`], a roaring-style bitmap with rank/select support
//! that's also used by fast-field columns elsewhere in tantivy.
//!
//! ## On-disk layout
//!
//! ```text
//! [u8 variant_tag] [body]
//!   tag = 0  (Full):     no body — `num_docs` comes from the caller
//!                        (typically `segment_reader.max_doc()`)
//!   tag = 1  (Optional): body = serialized columnar OptionalIndex
//! ```

use std::io::{self, Write};

use columnar::column_index::{open_optional_index, serialize_optional_index, OptionalIndex, Set};
use common::HasLen;

use crate::directory::FileSlice;
use crate::DocId;

const VARIANT_FULL: u8 = 0;
const VARIANT_OPTIONAL: u8 = 1;

/// Per-field presence tracker. Dispatches on cardinality at open time so
/// the hot path can skip the bitmap entirely when every doc has a value.
pub enum Presence {
    /// Every doc has a value. `row_id == doc_id`; no bitmap stored.
    Full { num_docs: u32 },
    /// Some docs may be absent. Rank/contains go through columnar's
    /// `OptionalIndex` (roaring-style block bitmap).
    Optional(OptionalIndex),
}

impl Presence {
    /// Serialize the appropriate variant given a sorted list of present
    /// `doc_id`s. Chooses `Full` if every doc is present, `Optional`
    /// otherwise.
    ///
    /// The Full variant writes only the variant tag — `num_docs` is
    /// supplied at open time (typically from `segment_reader.max_doc()`).
    pub fn serialize<W: Write>(
        present_doc_ids: &[DocId],
        num_docs: u32,
        out: &mut W,
    ) -> io::Result<()> {
        if present_doc_ids.len() == num_docs as usize {
            out.write_all(&[VARIANT_FULL])?;
        } else {
            out.write_all(&[VARIANT_OPTIONAL])?;
            serialize_optional_index(&present_doc_ids, num_docs, out)?;
        }
        Ok(())
    }

    /// Parse a serialized presence section, dispatching on the variant tag.
    /// `num_docs` is used only when the variant is `Full` — for `Optional`,
    /// the count is read from the embedded `OptionalIndex` header.
    pub fn open(file_slice: FileSlice, num_docs: u32) -> io::Result<Presence> {
        if file_slice.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "presence section is empty",
            ));
        }
        let tag = file_slice.slice(0..1).read_bytes()?[0];
        let body = file_slice.slice_from(1);
        match tag {
            VARIANT_FULL => Ok(Presence::Full { num_docs }),
            VARIANT_OPTIONAL => Ok(Presence::Optional(open_optional_index(body)?)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown presence variant tag: {other}"),
            )),
        }
    }

    /// Number of docs that have a value.
    pub fn num_non_null(&self) -> u32 {
        match self {
            Presence::Full { num_docs } => *num_docs,
            Presence::Optional(idx) => idx.num_non_nulls(),
        }
    }

    /// `true` if `doc_id` has a value.
    #[inline]
    pub fn contains(&self, doc_id: DocId) -> bool {
        match self {
            Presence::Full { num_docs } => doc_id < *num_docs,
            Presence::Optional(idx) => Set::contains(idx, doc_id),
        }
    }

    /// Returns the dense row id for `doc_id` if it has a value, else `None`.
    /// For `Full`, this is the identity map — no bitmap consulted.
    /// Callers must pass a `doc_id` within the segment (`doc_id < max_doc`);
    /// this is asserted in debug builds.
    #[inline]
    pub fn rank_if_exists(&self, doc_id: DocId) -> Option<u32> {
        match self {
            Presence::Full { num_docs } => {
                debug_assert!(doc_id < *num_docs, "doc_id {doc_id} >= num_docs {num_docs}");
                Some(doc_id)
            }
            Presence::Optional(idx) => Set::rank_if_exists(idx, doc_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(present: &[DocId], num_docs: u32) -> Presence {
        let mut buf = Vec::new();
        Presence::serialize(present, num_docs, &mut buf).unwrap();
        Presence::open(FileSlice::from(buf), num_docs).unwrap()
    }

    #[test]
    fn test_all_present_uses_full_variant() {
        let n = 100u32;
        let present: Vec<DocId> = (0..n).collect();

        // Wire-level: the serialized output is exactly 1 byte (just the
        // variant tag); no body — num_docs comes from the caller.
        let mut buf = Vec::new();
        Presence::serialize(&present, n, &mut buf).unwrap();
        assert_eq!(buf.len(), 1, "Full variant should write only the tag");
        assert_eq!(buf[0], VARIANT_FULL);

        let p = Presence::open(FileSlice::from(buf), n).unwrap();
        assert!(matches!(p, Presence::Full { num_docs } if num_docs == n));
        assert_eq!(p.num_non_null(), n);
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
    fn test_none_present_uses_optional_variant() {
        let p = round_trip(&[], 100);
        assert!(matches!(p, Presence::Optional(_)));
        assert_eq!(p.num_non_null(), 0);
        for d in 0..100 {
            assert!(!p.contains(d));
            assert_eq!(p.rank_if_exists(d), None);
        }
    }

    #[test]
    fn test_sparse_uses_optional_variant() {
        let present: Vec<DocId> = vec![3, 7, 11, 12, 50, 99];
        let p = round_trip(&present, 100);
        assert!(matches!(p, Presence::Optional(_)));
        assert_eq!(p.num_non_null(), 6);
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
    fn test_optional_across_blocks() {
        // Exercise multiple roaring-style blocks (each spans 64K docs).
        let n = 1500u32;
        let present: Vec<DocId> = (0..n).filter(|d| d % 3 == 0).collect();
        let p = round_trip(&present, n);
        assert!(matches!(p, Presence::Optional(_)));
        assert_eq!(p.num_non_null() as usize, present.len());
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
}
