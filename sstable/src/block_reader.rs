use std::io::{self, Read};
use std::ops::Range;

use common::OwnedBytes;
use zstd::bulk::Decompressor as ZstdDecompressor;

use crate::delta::COMPRESSION_FLAG;

pub struct BlockReader {
    buffer: Vec<u8>,
    reader: OwnedBytes,
    offset: usize,
}

impl BlockReader {
    pub fn new(reader: OwnedBytes) -> BlockReader {
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

        let block_len = match self.reader.len() {
            0 => return Ok(false),
            1..=3 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to read block_len",
                ))
            }
            _ => self.reader.read_u32(),
        };
        if block_len == 0 {
            return Ok(false);
        }

        let real_block_len = (block_len & !COMPRESSION_FLAG) as usize;
        if self.reader.len() < real_block_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to read block content",
            ));
        }
        if real_block_len as u32 != block_len {
            // TODO this way of growing the Vec isn't very good, but using the friendlier APIs
            // comes with an important performance hit. Maybe we should encode the uncompressed
            // size of block somewhere, so we can know upfront.
            // In practice, it's rare to hit a 4:1 compression ratio, and the buffer is kept
            // arround so these reallocation are amortized.
            if self.buffer.capacity() < real_block_len * 4 {
                self.buffer.reserve((real_block_len) * 5);
            }
            loop {
                match ZstdDecompressor::new()
                    .unwrap()
                    .decompress_to_buffer::<Vec<u8>>(
                        &self.reader[..real_block_len],
                        &mut self.buffer,
                    ) {
                    Ok(_) => break,
                    Err(e) if e.kind() == io::ErrorKind::Other => {
                        if self.buffer.capacity() < 1024 * 1024 {
                            self.buffer.reserve(self.buffer.capacity() * 2);
                        } else {
                            return Err(e);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }

            self.reader.advance(real_block_len);
        } else {
            self.buffer.resize(real_block_len, 0u8);
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

impl io::Read for BlockReader {
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
