use std::io::{self, Read, Write};
use common::{VInt, BinarySerializable};

mod termdict;
mod streamer;
mod delta_encoder;


pub use self::delta_encoder::{TermDeltaDecoder, TermDeltaEncoder};
pub use self::delta_encoder::{DeltaTermInfo, TermInfoDeltaDecoder, TermInfoDeltaEncoder};

pub use self::termdict::TermDictionaryImpl;
pub use self::termdict::TermDictionaryBuilderImpl;
pub use self::streamer::TermStreamerImpl;
pub use self::streamer::TermStreamerBuilderImpl;

#[derive(Debug)]
pub struct CheckPoint {
    pub stream_offset: u64,
    pub postings_offset: u64,
    pub positions_offset: u64,
}

impl BinarySerializable for CheckPoint {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.stream_offset).serialize(writer)?;
        VInt(self.postings_offset).serialize(writer)?;
        VInt(self.positions_offset).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let stream_offset = VInt::deserialize(reader)?.0;
        let postings_offset = VInt::deserialize(reader)?.0;
        let positions_offset = VInt::deserialize(reader)?.0;
        Ok(CheckPoint {
            stream_offset,
            postings_offset,
            positions_offset,
        })
    }
}
