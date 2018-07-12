use compression::compressed_block_size;
use compression::BlockDecoder;
use compression::COMPRESSION_BLOCK_SIZE;
use directory::ReadOnlySource;
use owned_read::OwnedRead;

/// Reads a stream of compressed ints.
///
/// Tantivy uses `CompressedIntStream` to read
/// the position file.
/// The `.skip(...)` makes it possible to avoid
/// decompressing blocks that are not required.
pub struct CompressedIntStream {
    buffer: OwnedRead,

    block_decoder: BlockDecoder,
    cached_addr: usize,      // address of the currently decoded block
    cached_next_addr: usize, // address following the currently decoded block

    addr: usize, // address of the block associated to the current position
    inner_offset: usize,
}

impl CompressedIntStream {
    /// Opens a compressed int stream.
    pub(crate) fn wrap(source: ReadOnlySource) -> CompressedIntStream {
        CompressedIntStream {
            buffer: OwnedRead::new(source),
            block_decoder: BlockDecoder::new(),
            cached_addr: usize::max_value(),
            cached_next_addr: usize::max_value(),

            addr: 0,
            inner_offset: 0,
        }
    }

    /// Loads the block at the given address and return the address of the
    /// following block
    pub fn read_block(&mut self, addr: usize) -> usize {
        if self.cached_addr == addr {
            // we are already on this block.
            // no need to read.
            self.cached_next_addr
        } else {
            let next_addr = addr + self.block_decoder
                .uncompress_block_unsorted(self.buffer.slice_from(addr));
            self.cached_addr = addr;
            self.cached_next_addr = next_addr;
            next_addr
        }
    }

    /// Fills a buffer with the next `output.len()` integers.
    /// This does not consume / advance the stream.
    pub fn read(&mut self, output: &mut [u32]) {
        let mut cursor = self.addr;
        let mut inner_offset = self.inner_offset;
        let mut num_els: usize = output.len();
        let mut start = 0;
        loop {
            cursor = self.read_block(cursor);
            let block = &self.block_decoder.output_array()[inner_offset..];
            let block_len = block.len();
            if num_els >= block_len {
                output[start..start + block_len].clone_from_slice(&block);
                start += block_len;
                num_els -= block_len;
                inner_offset = 0;
            } else {
                output[start..].clone_from_slice(&block[..num_els]);
                break;
            }
        }
    }

    /// Skip the next `skip_len` integer.
    ///
    /// If a full block is skipped, calling
    /// `.skip(...)` will avoid decompressing it.
    ///
    /// May panic if the end of the stream is reached.
    pub fn skip(&mut self, mut skip_len: usize) {
        loop {
            let available = COMPRESSION_BLOCK_SIZE - self.inner_offset;
            if available >= skip_len {
                self.inner_offset += skip_len;
                break;
            } else {
                skip_len -= available;
                // entirely skip decompressing some blocks.
                let num_bits: u8 = self.buffer.get(self.addr);
                let block_len = 1 + compressed_block_size(num_bits);
                self.addr += block_len;
                self.inner_offset = 0;
            }
        }
    }
}
