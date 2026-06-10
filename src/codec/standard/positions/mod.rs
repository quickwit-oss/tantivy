use std::io;

use common::OwnedBytes;

use crate::codec::positions::{PositionsCodec, PositionsReader, PositionsSerializer};
use crate::positions::{PositionReader, PositionSerializer};

/// The default positions codec for tantivy.
pub struct StandardPositionsCodec;

impl PositionsCodec for StandardPositionsCodec {
    type Serializer<W: io::Write> = PositionSerializer<W>;
    type Reader = PositionReader;

    fn new_serializer<W: io::Write>(&self, writer: W) -> Self::Serializer<W> {
        PositionSerializer::new(writer)
    }

    fn open_reader(&self, data: OwnedBytes) -> io::Result<Self::Reader> {
        PositionReader::open(data)
    }
}

impl<W: io::Write> PositionsSerializer<W> for PositionSerializer<W> {
    fn written_bytes(&self) -> u64 {
        PositionSerializer::written_bytes(self)
    }

    fn write_positions_delta(&mut self, positions_delta: &[u32]) {
        PositionSerializer::write_positions_delta(self, positions_delta);
    }

    fn close_term(&mut self) -> io::Result<()> {
        PositionSerializer::close_term(self)
    }

    fn close(self) -> io::Result<()> {
        PositionSerializer::close(self)
    }
}

impl PositionsReader for PositionReader {
    fn read(&mut self, offset: u64, output: &mut [u32]) {
        PositionReader::read(self, offset, output);
    }

    fn clone_box(&self) -> Box<dyn PositionsReader> {
        Box::new(self.clone())
    }
}
