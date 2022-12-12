use std::io::{self, Read};

use byteorder::{LittleEndian, ReadBytesExt};

pub struct BlockReader<'a> {
    buffer: Vec<u8>,
    reader: Box<dyn io::Read + 'a>,
    offset: usize,
}

impl<'a> BlockReader<'a> {
    pub fn new(reader: Box<dyn io::Read + 'a>) -> BlockReader<'a> {
        BlockReader {
            buffer: Vec::new(),
            reader,
            offset: 0,
        }
    }

    pub fn deserialize_u64(&mut self) -> u64 {
        let (num_bytes, val) = super::vint::deserialize_read(self.buffer());
        self.advance(num_bytes);
        val
    }

    #[inline(always)]
    pub fn buffer_from_to(&self, start: usize, end: usize) -> &[u8] {
        &self.buffer[start..end]
    }

    pub fn read_block(&mut self) -> io::Result<bool> {
        self.offset = 0;
        let block_len_res = self.reader.read_u32::<LittleEndian>();
        if let Err(err) = &block_len_res {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(false);
            }
        }
        let block_len = block_len_res?;
        if block_len == 0u32 {
            self.buffer.clear();
            return Ok(false);
        }
        self.buffer.resize(block_len as usize, 0u8);
        self.reader.read_exact(&mut self.buffer[..])?;
        Ok(true)
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn advance(&mut self, num_bytes: usize) {
        self.offset += num_bytes;
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }
}

impl<'a> io::Read for BlockReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.buffer().read(buf)?;
        self.advance(len);
        Ok(len)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let len = self.buffer.len();
        buf.extend_from_slice(self.buffer());
        self.advance(len);
        Ok(len)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.buffer().read_exact(buf)?;
        self.advance(buf.len());
        Ok(())
    }
}
