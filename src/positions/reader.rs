use crate::common::{BinarySerializable, FixedSize};
use crate::directory::ReadOnlySource;
use crate::positions::COMPRESSION_BLOCK_SIZE;
use crate::positions::LONG_SKIP_INTERVAL;
use crate::positions::LONG_SKIP_IN_BLOCKS;
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
use bitpacking::{BitPacker, BitPacker4x};
use owned_read::OwnedRead;

struct Positions {
    bit_packer: BitPacker4x,
    skip_source: ReadOnlySource,
    position_source: ReadOnlySource,
    long_skip_source: ReadOnlySource,
}

impl Positions {
    pub fn new(position_source: ReadOnlySource, skip_source: ReadOnlySource) -> Positions {
        let (body, footer) = skip_source.split_from_end(u32::SIZE_IN_BYTES);
        let num_long_skips = u32::deserialize(&mut footer.as_slice()).expect("Index corrupted");
        let (skip_source, long_skip_source) =
            body.split_from_end(u64::SIZE_IN_BYTES * (num_long_skips as usize));
        Positions {
            bit_packer: BitPacker4x::new(),
            skip_source,
            long_skip_source,
            position_source,
        }
    }

    /// Returns the offset of the block associated to the given `long_skip_id`.
    ///
    /// One `long_skip_id` means `LONG_SKIP_IN_BLOCKS` blocks.
    fn long_skip(&self, long_skip_id: usize) -> u64 {
        if long_skip_id == 0 {
            return 0;
        }
        let long_skip_slice = self.long_skip_source.as_slice();
        let mut long_skip_blocks: &[u8] = &long_skip_slice[(long_skip_id - 1) * 8..][..8];
        u64::deserialize(&mut long_skip_blocks).expect("Index corrupted")
    }

    fn reader(&self, offset: u64) -> PositionReader {
        let long_skip_id = (offset / LONG_SKIP_INTERVAL) as usize;
        let offset_num_bytes: u64 = self.long_skip(long_skip_id);
        let mut position_read = OwnedRead::new(self.position_source.clone());
        position_read.advance(offset_num_bytes as usize);
        let mut skip_read = OwnedRead::new(self.skip_source.clone());
        skip_read.advance(long_skip_id * LONG_SKIP_IN_BLOCKS);
        PositionReader {
            bit_packer: self.bit_packer,
            skip_read,
            position_read,
            buffer: Box::new([0u32; 128]),
            block_offset: std::i64::MAX as u64,
            anchor_offset: (long_skip_id as u64) * LONG_SKIP_INTERVAL,
            abs_offset: offset,
        }
    }
}

#[derive(Clone)]
pub struct PositionReader {
    skip_read: OwnedRead,
    position_read: OwnedRead,
    bit_packer: BitPacker4x,
    buffer: Box<[u32; COMPRESSION_BLOCK_SIZE]>,

    block_offset: u64,
    anchor_offset: u64,

    abs_offset: u64,
}

impl PositionReader {
    pub fn new(
        position_source: ReadOnlySource,
        skip_source: ReadOnlySource,
        offset: u64,
    ) -> PositionReader {
        Positions::new(position_source, skip_source).reader(offset)
    }

    fn advance_num_blocks(&mut self, num_blocks: usize) {
        let num_bits: usize = self.skip_read.as_ref()[..num_blocks]
            .iter()
            .cloned()
            .map(|num_bits| num_bits as usize)
            .sum();
        let num_bytes_to_skip = num_bits * COMPRESSION_BLOCK_SIZE / 8;
        self.skip_read.advance(num_blocks as usize);
        self.position_read.advance(num_bytes_to_skip);
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
        if delta_to_block_offset < 0 || delta_to_block_offset >= 128 {
            // The first position is not within the first block.
            // We need to decompress the first block.
            let delta_to_anchor_offset = offset - self.anchor_offset;
            let num_blocks_to_skip =
                (delta_to_anchor_offset / (COMPRESSION_BLOCK_SIZE as u64)) as usize;
            self.advance_num_blocks(num_blocks_to_skip);
            self.anchor_offset = offset - (offset % COMPRESSION_BLOCK_SIZE as u64);
            self.block_offset = self.anchor_offset;
            let num_bits = self.skip_read.get(0);
            self.bit_packer
                .decompress(self.position_read.as_ref(), self.buffer.as_mut(), num_bits);
        } else {
            let num_blocks_to_skip =
                ((self.block_offset - self.anchor_offset) / COMPRESSION_BLOCK_SIZE as u64) as usize;
            self.advance_num_blocks(num_blocks_to_skip);
            self.anchor_offset = self.block_offset;
        }

        let mut num_bits = self.skip_read.get(0);
        let mut position_data = self.position_read.as_ref();

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
            num_bits = self.skip_read.get(i);
            self.bit_packer
                .decompress(position_data, self.buffer.as_mut(), num_bits);
            self.block_offset += COMPRESSION_BLOCK_SIZE as u64;
        }
    }
}
