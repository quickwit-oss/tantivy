use std::io::{self, Read};
use std::ops::Range;

use common::OwnedBytes;
#[cfg(feature = "zstd-compression")]
use zstd::bulk::Decompressor;

pub struct BlockReader {
    buffer: Vec<u8>,
    reader: OwnedBytes,
    next_readers: std::vec::IntoIter<(OwnedBytes, u64)>,
    offset: usize,
    /// First term ordinal of the slice we just moved to, taken once by the caller.
    ///
    /// When an automaton prunes blocks, the slices handed to us are not contiguous, so a
    /// caller counting terms cannot know the ordinal it is at. Each slice therefore carries
    /// the ordinal of its first term; consecutive blocks merged into one slice stay
    /// contiguous, so counting within a slice remains correct.
    pending_first_ordinal: Option<u64>,
}

impl BlockReader {
    pub fn new(reader: OwnedBytes) -> BlockReader {
        BlockReader {
            buffer: Vec::new(),
            reader,
            next_readers: Vec::new().into_iter(),
            offset: 0,
            pending_first_ordinal: None,
        }
    }

    /// Build a reader over non-contiguous slices, each labelled with the term ordinal of its
    /// first term. See [`BlockReader::take_first_ordinal`].
    pub fn from_multiple_blocks(readers: Vec<(OwnedBytes, u64)>) -> BlockReader {
        let mut next_readers = readers.into_iter();
        let (reader, first_ordinal) = next_readers
            .next()
            .unwrap_or_else(|| (OwnedBytes::empty(), 0));
        BlockReader {
            buffer: Vec::new(),
            reader,
            next_readers,
            offset: 0,
            pending_first_ordinal: Some(first_ordinal),
        }
    }

    /// The first term ordinal of the slice most recently moved to, if it has not been taken yet.
    ///
    /// Returns `Some` exactly once per slice, on the first term read from it, so a caller can
    /// reset its ordinal counter instead of incrementing across the gap left by pruned blocks.
    pub fn take_first_ordinal(&mut self) -> Option<u64> {
        self.pending_first_ordinal.take()
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

        loop {
            let block_len = match self.reader.len() {
                0 => {
                    // we are out of data for this block. Check if we have another block after
                    match self.next_readers.next() {
                        Some((new_reader, first_ordinal)) => {
                            self.reader = new_reader;
                            self.pending_first_ordinal = Some(first_ordinal);
                            continue;
                        }
                        _ => {
                            return Ok(false);
                        }
                    }
                }
                1..=3 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to read block_len",
                    ));
                }
                _ => self.reader.read_u32() as usize,
            };
            if block_len <= 1 {
                return Ok(false);
            }
            let compress = self.reader.read_u8();
            let block_len = block_len - 1;

            if self.reader.len() < block_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to read block content",
                ));
            }
            if compress == 1 {
                #[cfg(feature = "zstd-compression")]
                {
                    let required_capacity =
                        Decompressor::upper_bound(&self.reader[..block_len]).unwrap_or(1024 * 1024);
                    self.buffer.reserve(required_capacity);
                    Decompressor::new()?
                        .decompress_to_buffer(&self.reader[..block_len], &mut self.buffer)?;

                    self.reader.advance(block_len);
                }

                if cfg!(not(feature = "zstd-compression")) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "zstd-compression feature is not enabled",
                    ));
                }
            } else {
                self.buffer.resize(block_len, 0u8);
                self.reader.read_exact(&mut self.buffer[..])?;
            }

            return Ok(true);
        }
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
