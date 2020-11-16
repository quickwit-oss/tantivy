use std::io;
use super::BLOCK_LEN;
use byteorder::{LittleEndian, ReadBytesExt};

pub struct BlockReader<'a> {
    buffer: Vec<u8>,
    reader: Box<dyn io::Read + 'a>
}

impl<'a> BlockReader<'a> {

    pub fn new(reader: Box<dyn io::Read + 'a>) -> BlockReader<'a> {
        BlockReader {
            buffer: Vec::with_capacity(BLOCK_LEN),
            reader
        }
    }

    pub fn read_block(&mut self) -> io::Result<bool> {
        let block_len = self.reader.read_u32::<LittleEndian>()?;
        if block_len == 0u32 {
            self.buffer.clear();
            Ok(false)
        } else {
            self.buffer.resize(block_len as usize, 0u8);
            self.reader.read_exact(&mut self.buffer[..])?;
            Ok(true)
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}
