use std::io;
use postings::TermInfo;
use common::BinarySerializable;

struct TermInfoStore {

}

pub struct TermInfoStoreWriter {
    buffer: Vec<u8>,
}

impl TermInfoStoreWriter {
    pub fn new() -> TermInfoStoreWriter {
        TermInfoStoreWriter {
            buffer: Vec::new(),
        }
    }

    pub fn write_term_info(&mut self, term_info: &TermInfo) -> io::Result<()> {
        term_info.serialize(&mut self.buffer)?;
        Ok(())
    }

    pub fn serialize<W: io::Write>(&mut self, write: &mut W) -> io::Result<u64> {
        let len = self.buffer.len() as u64;
        write.write_all(&self.buffer)?;
        Ok(len)
    }
}

