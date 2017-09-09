use std::io::{self, Write, Read};
use common::BinarySerializable;

mod termdict;
mod streamer;
mod delta_encoder;


pub use self::delta_encoder::{TermDeltaEncoder, TermDeltaDecoder};
pub use self::delta_encoder::{TermInfoDeltaEncoder, TermInfoDeltaDecoder, DeltaTermInfo};

pub use self::termdict::TermDictionaryImpl;
pub use self::termdict::TermDictionaryBuilderImpl;
pub use self::streamer::TermStreamerImpl;
pub use self::streamer::TermStreamerBuilderImpl;

#[derive(Debug)]
pub struct CheckPoint {
    pub stream_offset: u32,
    pub postings_offset: u32,
    pub positions_offset: u32,
}

impl BinarySerializable for CheckPoint {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.stream_offset.serialize(writer)?;
        self.postings_offset.serialize(writer)?;
        self.positions_offset.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let stream_offset = u32::deserialize(reader)?;
        let postings_offset = u32::deserialize(reader)?;
        let positions_offset = u32::deserialize(reader)?;
        Ok(CheckPoint {
            stream_offset: stream_offset,
            postings_offset: postings_offset,
            positions_offset: positions_offset,
        })
    }
}
