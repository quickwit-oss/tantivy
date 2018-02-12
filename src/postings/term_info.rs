use common::{BinarySerializable, FixedSize};
use std::io;

/// `TermInfo` contains all of the information
/// associated to terms in the `.term` file.
///
/// It consists of
/// * `doc_freq` : the number of document in the segment
/// containing this term. It is also the length of the
/// posting list associated to this term
/// * `postings_offset` : an offset in the `.idx` file
/// addressing the start of the posting list associated
/// to this term.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct TermInfo {
    /// Number of documents in the segment containing the term
    pub doc_freq: u32,
    /// Offset within the postings (`.idx`) file.
    pub postings_offset: u64,
    /// Offset within the position (`.pos`) file.
    pub positions_offset: u64,
    /// Offset within the position block.
    pub positions_inner_offset: u8,
}

impl FixedSize for TermInfo {
    /// Size required for the binary serialization of `TermInfo`.
    /// This is large, but in practise, all `TermInfo` but the first one
    /// of the block are bitpacked.
    ///
    /// See `TermInfoStore`.
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
