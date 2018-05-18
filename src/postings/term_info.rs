use common::{BinarySerializable, FixedSize};
use std::io;

/// `TermInfo` wraps the metadata associated to a Term.
/// It is segment-local.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct TermInfo {
    /// Number of documents in the segment containing the term
    pub doc_freq: u32,
    /// Start offset within the postings (`.idx`) file.
    pub postings_offset: u64,
    /// Start offset of the first block within the position (`.pos`) file.
    pub positions_offset: u64,
    /// Start offset within this position block.
    pub positions_inner_offset: u8,
}

impl FixedSize for TermInfo {
    /// Size required for the binary serialization of a `TermInfo` object.
    /// This is large, but in practise, `TermInfo` are encoded in blocks and
    /// only the first `TermInfo` of a block is serialized uncompressed.
    /// The subsequent `TermInfo` are delta encoded and bitpacked.
    const SIZE_IN_BYTES: usize = u32::SIZE_IN_BYTES + 2 * u64::SIZE_IN_BYTES + u8::SIZE_IN_BYTES;
}

impl BinarySerializable for TermInfo {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        self.doc_freq.serialize(writer)?;
        self.postings_offset.serialize(writer)?;
        self.positions_offset.serialize(writer)?;
        self.positions_inner_offset.serialize(writer)
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let doc_freq = u32::deserialize(reader)?;
        let postings_offset = u64::deserialize(reader)?;
        let positions_offset = u64::deserialize(reader)?;
        let positions_inner_offset = u8::deserialize(reader)?;
        Ok(TermInfo {
            doc_freq,
            postings_offset,
            positions_offset,
            positions_inner_offset,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::TermInfo;
    use common::test::fixed_size_test;

    #[test]
    fn test_fixed_size() {
        fixed_size_test::<TermInfo>();
    }
}
