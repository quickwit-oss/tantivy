use crate::common::{BinarySerializable, FixedSize};
use std::io;

/// `TermInfo` wraps the metadata associated to a Term.
/// It is segment-local.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct TermInfo {
    /// Number of documents in the segment containing the term
    pub doc_freq: u32,
    /// Start offset of the posting list within the postings (`.idx`) file.
    pub postings_start_offset: u64,
    /// Stop offset of the posting list within the postings (`.idx`) file.
    /// The byte range is `[start_offset..stop_offset)`.
    pub postings_stop_offset: u64,
    /// Start offset of the first block within the position (`.pos`) file.
    pub positions_idx: u64,
}

impl TermInfo {
    pub(crate) fn posting_num_bytes(&self) -> u32 {
        let num_bytes = self.postings_stop_offset - self.postings_start_offset;
        assert!(num_bytes <= std::u32::MAX as u64);
        num_bytes as u32
    }
}

impl FixedSize for TermInfo {
    /// Size required for the binary serialization of a `TermInfo` object.
    /// This is large, but in practise, `TermInfo` are encoded in blocks and
    /// only the first `TermInfo` of a block is serialized uncompressed.
    /// The subsequent `TermInfo` are delta encoded and bitpacked.
    const SIZE_IN_BYTES: usize = 2 * u32::SIZE_IN_BYTES + 2 * u64::SIZE_IN_BYTES;
}

impl BinarySerializable for TermInfo {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        self.doc_freq.serialize(writer)?;
        self.postings_start_offset.serialize(writer)?;
        self.posting_num_bytes().serialize(writer)?;
        self.positions_idx.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let doc_freq = u32::deserialize(reader)?;
        let postings_start_offset = u64::deserialize(reader)?;
        let postings_num_bytes = u32::deserialize(reader)?;
        let postings_stop_offset = postings_start_offset + u64::from(postings_num_bytes);
        let positions_idx = u64::deserialize(reader)?;
        Ok(TermInfo {
            doc_freq,
            postings_start_offset,
            postings_stop_offset,
            positions_idx,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::TermInfo;
    use crate::common::test::fixed_size_test;

    #[test]
    fn test_fixed_size() {
        fixed_size_test::<TermInfo>();
    }
}
