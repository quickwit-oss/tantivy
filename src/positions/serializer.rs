use crate::common::BinarySerializable;
use crate::common::CountingWriter;
use crate::positions::{COMPRESSION_BLOCK_SIZE, LONG_SKIP_INTERVAL};
use bitpacking::BitPacker;
use bitpacking::BitPacker4x;
use std::io::{self, Write};

pub struct PositionSerializer<W: io::Write> {
    bit_packer: BitPacker4x,
    write_stream: CountingWriter<W>,
    write_skip_index: W,
    block: Vec<u32>,
    buffer: Vec<u8>,
    num_ints: u64,
    long_skips: Vec<u64>,
}

impl<W: io::Write> PositionSerializer<W> {
    pub fn new(write_stream: W, write_skip_index: W) -> PositionSerializer<W> {
        PositionSerializer {
            bit_packer: BitPacker4x::new(),
            write_stream: CountingWriter::wrap(write_stream),
            write_skip_index,
            block: Vec::with_capacity(128),
            buffer: vec![0u8; 128 * 4],
            num_ints: 0u64,
            long_skips: Vec::new(),
        }
    }

    pub fn positions_idx(&self) -> u64 {
        self.num_ints
    }

    fn remaining_block_len(&self) -> usize {
        COMPRESSION_BLOCK_SIZE - self.block.len()
    }

    pub fn write_all(&mut self, mut vals: &[u32]) -> io::Result<()> {
        while !vals.is_empty() {
            let remaining_block_len = self.remaining_block_len();
            let num_to_write = remaining_block_len.min(vals.len());
            self.block.extend(&vals[..num_to_write]);
            self.num_ints += num_to_write as u64;
            vals = &vals[num_to_write..];
            if self.remaining_block_len() == 0 {
                self.flush_block()?;
            }
        }
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let num_bits = self.bit_packer.num_bits(&self.block[..]);
        self.write_skip_index.write_all(&[num_bits])?;
        let written_len = self
            .bit_packer
            .compress(&self.block[..], &mut self.buffer, num_bits);
        self.write_stream.write_all(&self.buffer[..written_len])?;
        self.block.clear();
        if (self.num_ints % LONG_SKIP_INTERVAL) == 0u64 {
            self.long_skips.push(self.write_stream.written_bytes());
        }
        Ok(())
    }

    pub fn close(mut self) -> io::Result<()> {
        if !self.block.is_empty() {
            self.block.resize(COMPRESSION_BLOCK_SIZE, 0u32);
            self.flush_block()?;
        }
        for &long_skip in &self.long_skips {
            long_skip.serialize(&mut self.write_skip_index)?;
        }
        (self.long_skips.len() as u32).serialize(&mut self.write_skip_index)?;
        self.write_skip_index.flush()?;
        self.write_stream.flush()?;
        Ok(())
    }
}
