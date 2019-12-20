use crate::common::{BinarySerializable, FixedSize};
use crate::directory::{AdvancingReadOnlySource, ReadOnlySource};
use crate::positions::COMPRESSION_BLOCK_SIZE;
use crate::positions::LONG_SKIP_INTERVAL;
use crate::positions::LONG_SKIP_IN_BLOCKS;
use crate::postings::compression::compressed_block_size;
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
use std::io::Read;

struct Positions {
    bit_packer: BitPacker4x,
    skip_source: ReadOnlySource,
    position_source: ReadOnlySource,
    long_skip_source: ReadOnlySource,
}

impl Positions {
    pub fn new(position_source: ReadOnlySource, skip_source: ReadOnlySource) -> Positions {
        let (body, mut footer) = skip_source.split_from_end(u32::SIZE_IN_BYTES);
        let num_long_skips = u32::deserialize(&mut footer).expect("Index corrupted");
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
        let mut long_skip_slice = self.long_skip_source.slice_from((long_skip_id - 1) * 8);
        u64::deserialize(&mut long_skip_slice).expect("Index corrupted")
    }

    fn reader(&self, offset: u64) -> PositionReader {
        let long_skip_id = (offset / LONG_SKIP_INTERVAL) as usize;
        let small_skip = (offset % LONG_SKIP_INTERVAL) as usize;
        let offset_num_bytes: u64 = self.long_skip(long_skip_id);
        let mut position_read = AdvancingReadOnlySource::from(self.position_source.clone());
        position_read.advance(offset_num_bytes as usize);
        let mut skip_read = AdvancingReadOnlySource::from(self.skip_source.clone());
        skip_read.advance(long_skip_id * LONG_SKIP_IN_BLOCKS);
        let mut position_reader = PositionReader {
            bit_packer: self.bit_packer,
            skip_read,
            position_read,
            inner_offset: 0,
            buffer: Box::new([0u32; 128]),
            ahead: None,
        };
        position_reader.skip(small_skip);
        position_reader
    }
}

pub struct PositionReader {
    skip_read: AdvancingReadOnlySource,
    position_read: AdvancingReadOnlySource,
    bit_packer: BitPacker4x,
    inner_offset: usize,
    buffer: Box<[u32; 128]>,
    ahead: Option<usize>, // if None, no block is loaded.
                          // if Some(num_blocks), the block currently loaded is num_blocks ahead
                          // of the block of the next int to read.
}

// `ahead` represents the offset of the block currently loaded
// compared to the cursor of the actual stream.
//
// By contract, when this function is called, the current block has to be
// decompressed.
//
// If the requested number of els ends exactly at a given block, the next
// block is not decompressed.
fn read_impl(
    bit_packer: BitPacker4x,
    mut position: AdvancingReadOnlySource,
    buffer: &mut [u32; 128],
    mut inner_offset: usize,
    num_bits: &[u8],
    output: &mut [u32],
) -> usize {
    let mut output_start = 0;
    let mut output_len = output.len();
    let mut ahead = 0;
    loop {
        let available_len = COMPRESSION_BLOCK_SIZE - inner_offset;
        // We have enough elements in the current block.
        // Let's copy the requested elements in the output buffer,
        // and return.
        if output_len <= available_len {
            output[output_start..].copy_from_slice(&buffer[inner_offset..][..output_len]);
            return ahead;
        }
        output[output_start..][..available_len].copy_from_slice(&buffer[inner_offset..]);
        output_len -= available_len;
        output_start += available_len;
        inner_offset = 0;
        let num_bits = num_bits[ahead];
        let block_len = compressed_block_size(num_bits);
        let mut position_buf = vec![0u8; block_len];
        position
            .read_exact(&mut position_buf)
            .expect("Can't read position data");
        bit_packer.decompress(&position_buf, &mut buffer[..], num_bits);
        ahead += 1;
    }
}

impl PositionReader {
    pub fn new(
        position_source: ReadOnlySource,
        skip_source: ReadOnlySource,
        offset: u64,
    ) -> PositionReader {
        Positions::new(position_source, skip_source).reader(offset)
    }

    /// Fills a buffer with the next `output.len()` integers.
    /// This does not consume / advance the stream.
    pub fn read(&mut self, output: &mut [u32]) {
        let mut skip_read = self.skip_read.clone();
        let mut num_bits = vec![0; 1];

        skip_read
            .read_exact(&mut num_bits)
            .expect("Can't read num bits");
        let num_bits = num_bits[0];
        let mut skip_data = Vec::new();
        skip_read
            .read_to_end(&mut skip_data)
            .expect("Can't read skip read source");

        let mut position_data = self.position_read.clone();
        let block_len = compressed_block_size(num_bits);
        let mut position_buf = vec![0u8; block_len];
        position_data
            .read_exact(&mut position_buf)
            .expect("Can't read position data");

        if self.ahead != Some(0) {
            // the block currently available is not the block
            // for the current position
            self.bit_packer
                .decompress(&position_buf, self.buffer.as_mut(), num_bits);
            self.ahead = Some(0);
        }
        self.ahead = Some(read_impl(
            self.bit_packer,
            position_data,
            self.buffer.as_mut(),
            self.inner_offset,
            &skip_data,
            output,
        ));
    }

    /// Skip the next `skip_len` integer.
    ///
    /// If a full block is skipped, calling
    /// `.skip(...)` will avoid decompressing it.
    ///
    /// May panic if the end of the stream is reached.
    pub fn skip(&mut self, skip_len: usize) {
        let skip_len_plus_inner_offset = skip_len + self.inner_offset;

        let num_blocks_to_advance = skip_len_plus_inner_offset / COMPRESSION_BLOCK_SIZE;
        self.inner_offset = skip_len_plus_inner_offset % COMPRESSION_BLOCK_SIZE;

        self.ahead = self.ahead.and_then(|num_blocks| {
            if num_blocks >= num_blocks_to_advance {
                Some(num_blocks - num_blocks_to_advance)
            } else {
                None
            }
        });

        let mut skip_len_buf = vec![0u8; num_blocks_to_advance];
        self.skip_read
            .read_exact(&mut skip_len_buf)
            .expect("Can't read skip source");

        let skip_len_in_bits = skip_len_buf
            .iter()
            .map(|num_bits| *num_bits as usize)
            .sum::<usize>()
            * COMPRESSION_BLOCK_SIZE;
        let skip_len_in_bytes = skip_len_in_bits / 8;
        self.position_read.advance(skip_len_in_bytes);
    }
}
