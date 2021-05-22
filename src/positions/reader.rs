use std::io;

use crate::common::{BinarySerializable, FixedSize};
use crate::directory::FileSlice;
use crate::directory::OwnedBytes;
use crate::positions::COMPRESSION_BLOCK_SIZE;
use crate::positions::LONG_SKIP_INTERVAL;
use crate::positions::LONG_SKIP_IN_BLOCKS;
use bitpacking::{BitPacker, BitPacker4x};
use tantivy_fst::{FakeArr, Ulen};

/// Positions works as a long sequence of compressed block.
/// All terms are chained one after the other.
///
/// When accessing the position of a term, we get a positions_idx from the `Terminfo`.
/// This means we need to skip to the `nth` positions efficiently.
///
/// This is done thanks to two levels of skiping that we refer to in the code
/// as `long_skip` and `short_skip`.
///
/// The `long_skip` makes it possible to skip every 1_024 compression blocks (= 131_072 positions).
/// Skipping offset are simply stored one after as an offset stored over 8 bytes.
///
/// We find the number of long skips, as `n / long_skip`.
///
/// Blocks are compressed using bitpacking, so `skip_read` contains the number of bytes
/// (values can go from 0bit to 32 bits) required to decompressed every block.
///
/// A given block obviously takes `(128 x  num_bit_for_the_block / num_bits_in_a_byte)`,
/// so skipping a block without decompressing it is just a matter of advancing that many
/// bytes.

struct Positions {
    bit_packer: BitPacker4x,
    skip_file: FileSlice,
    position_file: FileSlice,
    long_skip_data: FileSlice,
}

impl Positions {
    pub fn new(position_file: FileSlice, skip_file: FileSlice) -> io::Result<Positions> {
        let (body, footer) = skip_file.split_from_end(u32::SIZE_IN_BYTES);
        let footer_data = footer.read_bytes()?;
        let num_long_skips = u32::deserialize(&mut footer_data.as_slice())?;
        let (skip_file, long_skip_file) =
            body.split_from_end(u64::SIZE_IN_BYTES * (num_long_skips as Ulen));
        Ok(Positions {
            bit_packer: BitPacker4x::new(),
            skip_file,
            long_skip_data: long_skip_file,
            position_file,
        })
    }

    /// Returns the offset of the block associated to the given `long_skip_id`.
    ///
    /// One `long_skip_id` means `LONG_SKIP_IN_BLOCKS` blocks.
    fn long_skip(&self, long_skip_id: Ulen) -> u64 {
        if long_skip_id == 0 {
            return 0;
        }
        let from = (long_skip_id - 1) * 8;
        let mut long_skip_blocks: &[u8] = &self.long_skip_data.slice(from, from + 8).to_vec();
        u64::deserialize(&mut long_skip_blocks).expect("Index corrupted")
    }

    fn reader(&self, offset: u64) -> io::Result<PositionReader> {
        let long_skip_id = (offset / LONG_SKIP_INTERVAL) as Ulen;
        let offset_num_bytes: u64 = self.long_skip(long_skip_id);
        let position_read = self
            .position_file
            .slice_from(offset_num_bytes as Ulen);
        let skip_read = self
            .skip_file
            .slice_from(long_skip_id * LONG_SKIP_IN_BLOCKS as Ulen);
        Ok(PositionReader {
            bit_packer: self.bit_packer,
            skip_read,
            position_read,
            buffer: Box::new([0u32; 128]),
            block_offset: std::i64::MAX as u64,
            anchor_offset: (long_skip_id as u64) * LONG_SKIP_INTERVAL,
            abs_offset: offset,
        })
    }
}

#[derive(Clone)]
pub struct PositionReader {
    skip_read: FileSlice,
    position_read: FileSlice,
    bit_packer: BitPacker4x,
    buffer: Box<[u32; COMPRESSION_BLOCK_SIZE as usize]>,

    block_offset: u64,
    anchor_offset: u64,

    abs_offset: u64,
}

impl PositionReader {
    pub fn new(
        position_file: FileSlice,
        skip_file: FileSlice,
        offset: u64,
    ) -> io::Result<PositionReader> {
        let positions = Positions::new(position_file, skip_file)?;
        positions.reader(offset)
    }

    fn advance_num_blocks(&mut self, num_blocks: Ulen) {
        let num_bits: usize = self.skip_read.slice(0, num_blocks).to_vec()
            .iter()
            .cloned()
            .map(|num_bits| num_bits as usize)
            .sum();
        let num_bytes_to_skip = num_bits * COMPRESSION_BLOCK_SIZE / 8;
        self.skip_read.advance(num_blocks as Ulen);
        self.position_read.advance(num_bytes_to_skip as Ulen);
    }

    /// Fills a buffer with the positions `[offset..offset+output.len())` integers.
    ///
    /// `offset` is required to have a value >= to the offsets given in previous calls
    /// for the given `PositionReaderAbsolute` instance.
    pub fn read(&mut self, mut offset: u64, mut output: &mut [u32]) {
        offset += self.abs_offset;
        assert!(
            offset >= self.anchor_offset,
            "offset arguments should be increasing."
        );
        let delta_to_block_offset = offset as i64 - self.block_offset as i64;
        if !(0..128).contains(&delta_to_block_offset) {
            // The first position is not within the first block.
            // We need to decompress the first block.
            let delta_to_anchor_offset = offset - self.anchor_offset;
            let num_blocks_to_skip =
                (delta_to_anchor_offset / (COMPRESSION_BLOCK_SIZE as u64)) as Ulen;
            self.advance_num_blocks(num_blocks_to_skip);
            self.anchor_offset = offset - (offset % COMPRESSION_BLOCK_SIZE as u64);
            self.block_offset = self.anchor_offset;
            let num_bits = self.skip_read.get_byte(0);
            self.bit_packer
                .decompress(&self.position_read.to_vec(), self.buffer.as_mut(), num_bits);
        } else {
            let num_blocks_to_skip =
                ((self.block_offset - self.anchor_offset) / COMPRESSION_BLOCK_SIZE as u64) as Ulen;
            self.advance_num_blocks(num_blocks_to_skip);
            self.anchor_offset = self.block_offset;
        }

        let mut num_bits = self.skip_read.get_byte(0);
        let position_data = self.position_read.to_vec();
        let mut position_data = position_data.as_slice();

        for i in 1.. {
            let offset_in_block = (offset as usize) % COMPRESSION_BLOCK_SIZE;
            let remaining_in_block = COMPRESSION_BLOCK_SIZE - offset_in_block;
            if remaining_in_block >= output.len() {
                output.copy_from_slice(&self.buffer[offset_in_block..][..output.len()]);
                break;
            }
            output[..remaining_in_block].copy_from_slice(&self.buffer[offset_in_block..]);
            output = &mut output[remaining_in_block..];
            offset += remaining_in_block as u64;
            position_data = &position_data[(num_bits as usize * COMPRESSION_BLOCK_SIZE / 8)..];
            num_bits = self.skip_read.get_byte(i);
            self.bit_packer
                .decompress(position_data, self.buffer.as_mut(), num_bits);
            self.block_offset += COMPRESSION_BLOCK_SIZE as u64;
        }
    }
}
