use common::BinarySerializable;
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

impl TermInfo {
    /// Size required to encode the `TermInfo`.
    // TODO  make this smaller when positions are unused for instance.
    pub(crate) const SIZE_IN_BYTES: usize = 4 + 8 + 8 + 1;
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
