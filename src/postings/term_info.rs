use std::io;
use std::ops::Range;
use std::sync::Arc;

use common::{BinarySerializable, FixedSize};

/// A whole short postings list stored in its term dictionary block.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ShortPostings {
    /// Strictly increasing segment-local document IDs.
    pub doc_ids: Vec<u32>,
    /// One frequency per document, or empty when this term has no frequencies.
    pub term_freqs: Vec<u32>,
    /// Position deltas in document order, or empty when positions are absent.
    pub position_deltas: Vec<u32>,
}

impl ShortPostings {
    #[cfg(feature = "quickwit")]
    pub(crate) fn validate(&self) -> io::Result<()> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid short postings");
        if !(1..=32).contains(&self.doc_ids.len())
            || (!self.term_freqs.is_empty() && self.term_freqs.len() != self.doc_ids.len())
            || self
                .doc_ids
                .last()
                .is_some_and(|&doc| doc >= crate::TERMINATED)
            || self.doc_ids.windows(2).any(|docs| docs[0] >= docs[1])
            || self
                .term_freqs
                .iter()
                .any(|&freq| freq == 0 || freq >= 1 << 31)
        {
            return Err(invalid());
        }
        if !self.position_deltas.is_empty() {
            let mut deltas = self.position_deltas.as_slice();
            for &freq in &self.term_freqs {
                let positions = deltas.get(..freq as usize).ok_or_else(invalid)?;
                if freq == 1 && positions[0] >= 1 << 31 {
                    return Err(invalid());
                }
                positions.iter().try_fold(0u32, |pos, &delta| {
                    pos.checked_add(delta).ok_or_else(invalid)
                })?;
                deltas = &deltas[freq as usize..];
            }
            if !deltas.is_empty() {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

/// `TermInfo` wraps the metadata associated with a Term.
/// It is segment-local.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct TermInfo {
    /// Number of documents in the segment containing the term
    pub doc_freq: u32,
    /// Byte range of the posting list within the postings (`.idx`) file.
    pub postings_range: Range<usize>,
    /// Byte range of the positions of this terms in the positions (`.pos`) file.
    pub positions_range: Range<usize>,
    /// Whole postings stored in the dictionary. External ranges are empty when present.
    pub short_postings: Option<Arc<ShortPostings>>,
}

impl TermInfo {
    pub(crate) fn posting_num_bytes(&self) -> u32 {
        let num_bytes = self.postings_range.len();
        assert!(num_bytes <= u32::MAX as usize);
        num_bytes as u32
    }

    pub(crate) fn positions_num_bytes(&self) -> u32 {
        let num_bytes = self.positions_range.len();
        assert!(num_bytes <= u32::MAX as usize);
        num_bytes as u32
    }
}

impl FixedSize for TermInfo {
    /// Size required for the binary serialization of a `TermInfo` object.
    /// This is large, but in practise, `TermInfo` are encoded in blocks and
    /// only the first `TermInfo` of a block is serialized uncompressed.
    /// The subsequent `TermInfo` are delta encoded and bitpacked.
    const SIZE_IN_BYTES: usize = 3 * u32::SIZE_IN_BYTES + 2 * u64::SIZE_IN_BYTES;
}

impl BinarySerializable for TermInfo {
    fn serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        if self.short_postings.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "short postings require an SSTable",
            ));
        }
        self.doc_freq.serialize(writer)?;
        (self.postings_range.start as u64).serialize(writer)?;
        self.posting_num_bytes().serialize(writer)?;
        (self.positions_range.start as u64).serialize(writer)?;
        self.positions_num_bytes().serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let doc_freq = u32::deserialize(reader)?;
        let postings_start_offset = u64::deserialize(reader)? as usize;
        let postings_num_bytes = u32::deserialize(reader)? as usize;
        let postings_end_offset = postings_start_offset + postings_num_bytes;
        let positions_start_offset = u64::deserialize(reader)? as usize;
        let positions_num_bytes = u32::deserialize(reader)? as usize;
        let positions_end_offset = positions_start_offset + positions_num_bytes;
        Ok(TermInfo {
            doc_freq,
            postings_range: postings_start_offset..postings_end_offset,
            positions_range: positions_start_offset..positions_end_offset,
            short_postings: None,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::TermInfo;
    use crate::tests::fixed_size_test;

    // TODO add serialize/deserialize test for terminfo

    #[cfg(feature = "quickwit")]
    #[test]
    fn rejects_document_ids_at_or_above_termination() {
        for doc_ids in [vec![crate::TERMINATED], vec![crate::TERMINATED + 1]] {
            let short = super::ShortPostings {
                doc_ids,
                ..Default::default()
            };
            assert!(short.validate().is_err());
        }
    }

    #[cfg(feature = "quickwit")]
    #[test]
    fn test_short_postings_document_count_limits() {
        for (count, valid) in [(0, false), (1, true), (32, true), (33, false)] {
            let short = super::ShortPostings {
                doc_ids: (0..count).collect(),
                ..Default::default()
            };
            assert_eq!(short.validate().is_ok(), valid);
        }
    }

    #[test]
    fn test_fixed_size() {
        fixed_size_test::<TermInfo>();
    }
}
