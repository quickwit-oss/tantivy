use bitpacking::{BitPacker4x, BitPacker};
use owned_read::OwnedRead;
use common::{BinarySerializable, FixedSize};
use postings::compression::compressed_block_size;
use directory::ReadOnlySource;
use positions::COMPRESSION_BLOCK_SIZE;
use positions::LONG_SKIP_IN_BLOCKS;
use positions::LONG_SKIP_INTERVAL;
use super::BIT_PACKER;

pub struct PositionReader {
    skip_read: OwnedRead,
    position_read: OwnedRead,
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
    mut position: &[u8],
    buffer: &mut [u32; 128],
    mut inner_offset: usize,
    num_bits: &[u8],
    output: &mut [u32]) -> usize {
    let mut output_start = 0;
    let mut output_len = output.len();
    let mut ahead = 0;
    loop {
        let available_len = 128 - inner_offset;
        if output_len <= available_len {
            output[output_start..].copy_from_slice(&buffer[inner_offset..][..output_len]);
            return ahead;
        } else {
            output[output_start..][..available_len].copy_from_slice(&buffer[inner_offset..]);
            output_len -= available_len;
            output_start += available_len;
            inner_offset = 0;
            let num_bits = num_bits[ahead];
            BitPacker4x::new()
                .decompress(position, &mut buffer[..], num_bits);
            let block_len = compressed_block_size(num_bits);
            position = &position[block_len..];
            ahead += 1;
        }
    }
}


impl PositionReader {
    pub fn new(position_source: ReadOnlySource,
               skip_source: ReadOnlySource,
               offset: u64) -> PositionReader {
        let skip_len = skip_source.len();
        let (body, footer) = skip_source.split(skip_len - u32::SIZE_IN_BYTES);
        let num_long_skips = u32::deserialize(&mut footer.as_slice()).expect("Index corrupted");
        let body_split = body.len() - u64::SIZE_IN_BYTES * (num_long_skips as usize);
        let (skip_body, long_skips) = body.split(body_split);
        let long_skip_id = (offset / LONG_SKIP_INTERVAL) as usize;
        let small_skip = (offset - (long_skip_id as u64) * (LONG_SKIP_INTERVAL as u64)) as usize;
        let offset_num_bytes: u64 = {
            if long_skip_id > 0 {
                let mut long_skip_blocks: &[u8] = &long_skips.as_slice()[(long_skip_id - 1) * 8..][..8];
                u64::deserialize(&mut long_skip_blocks).expect("Index corrupted") * 16
            } else {
                0
            }
        };
        let mut position_read = OwnedRead::new(position_source);
        position_read.advance(offset_num_bytes as usize);
        let mut skip_read = OwnedRead::new(skip_body);
        skip_read.advance(long_skip_id  * LONG_SKIP_IN_BLOCKS);
        let mut position_reader = PositionReader {
            skip_read,
            position_read,
            inner_offset: 0,
            buffer: Box::new([0u32; 128]),
            ahead: None
        };
        position_reader.skip(small_skip);
        position_reader
    }

    /// Fills a buffer with the next `output.len()` integers.
    /// This does not consume / advance the stream.
    pub fn read(&mut self, output: &mut [u32]) {
        let skip_data = self.skip_read.as_ref();
        let position_data = self.position_read.as_ref();
        let num_bits = self.skip_read.get(0);
        if self.ahead != Some(0) {
            // the block currently available is not the block
            // for the current position
            BIT_PACKER.decompress(position_data, self.buffer.as_mut(), num_bits);
        }
        let block_len = compressed_block_size(num_bits);
        self.ahead = Some(read_impl(
            &position_data[block_len..],
            self.buffer.as_mut(),
            self.inner_offset,
            &skip_data[1..],
            output));
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

        self.ahead = self.ahead
            .and_then(|num_blocks| {
                if num_blocks >= num_blocks_to_advance {
                    Some(num_blocks_to_advance - num_blocks_to_advance)
                } else {
                    None
                }
            });

        let skip_len = self.skip_read
            .as_ref()[..num_blocks_to_advance]
            .iter()
            .cloned()
            .map(|num_bit| num_bit as usize)
            .sum::<usize>() * (COMPRESSION_BLOCK_SIZE / 8);

        self.skip_read.advance(num_blocks_to_advance);
        self.position_read.advance(skip_len);
    }
}
