use std::io::{self, Read};
use std::ops::Range;

use zstd::stream::read::Decoder as ZstdDecoder;

use crate::delta::COMPRESSION_FLAG;

pub struct BlockReader<'a> {
    buffer: Vec<u8>,
    reader: Box<dyn io::Read + 'a>,
    offset: usize,
}

#[inline]
fn read_u32(read: &mut dyn io::Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    read.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

struct ReadLimiter<T> {
    inner: T,
    budget_left: usize,
}

impl<T> ReadLimiter<T> {
    fn new(read: T, budget: usize) -> ReadLimiter<T> {
        ReadLimiter {
            inner: read,
            budget_left: budget,
        }
    }
}

impl<T: Read> Read for ReadLimiter<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let to_read = buf.len().min(self.budget_left);
        let read = self.inner.read(&mut buf[..to_read])?;
        self.budget_left -= read;
        Ok(read)
    }
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
    pub fn buffer_from_to(&self, range: Range<usize>) -> &[u8] {
        &self.buffer[range]
    }

    pub fn read_block(&mut self) -> io::Result<bool> {
        self.offset = 0;
        self.buffer.clear();

        let block_len = match read_u32(self.reader.as_mut()) {
            Ok(0) => return Ok(false),
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(err) => return Err(err),
        };

        let real_block_len = block_len & !COMPRESSION_FLAG;
        if real_block_len != block_len {
            ZstdDecoder::new(ReadLimiter::new(
                self.reader.as_mut(),
                real_block_len as usize,
            ))?
            .read_to_end(&mut self.buffer)?;
        } else {
            self.buffer.resize(real_block_len as usize, 0u8);
            self.reader.read_exact(&mut self.buffer[..])?;
        }

        Ok(true)
    }

    #[inline(always)]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[inline(always)]
    pub fn advance(&mut self, num_bytes: usize) {
        self.offset += num_bytes;
    }

    #[inline(always)]
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
