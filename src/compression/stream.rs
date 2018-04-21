use compression::compressed_block_size;
use compression::BlockDecoder;
use compression::COMPRESSION_BLOCK_SIZE;
use directory::{ReadOnlySource, SourceRead};

/// Reads a stream of compressed ints.
///
/// Tantivy uses `CompressedIntStream` to read
/// the position file.
/// The `.skip(...)` makes it possible to avoid
/// decompressing blocks that are not required.
pub struct CompressedIntStream {
    buffer: SourceRead,

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
            buffer: SourceRead::from(source),
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
                let block_len = compressed_block_size(num_bits);
                self.addr += block_len;
                self.inner_offset = 0;
            }
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::CompressedIntStream;
    use compression::compressed_block_size;
    use compression::BlockEncoder;
    use compression::COMPRESSION_BLOCK_SIZE;
    use directory::ReadOnlySource;

    fn create_stream_buffer() -> ReadOnlySource {
        let mut buffer: Vec<u8> = vec![];
        let mut encoder = BlockEncoder::new();
        let vals: Vec<u32> = (0u32..1152u32).collect();
        for chunk in vals.chunks(COMPRESSION_BLOCK_SIZE) {
            let compressed_block = encoder.compress_block_unsorted(chunk);
            let num_bits = compressed_block[0];
            assert_eq!(compressed_block_size(num_bits), compressed_block.len());
            buffer.extend_from_slice(compressed_block);
        }
        if cfg!(simd) {
            buffer.extend_from_slice(&[0u8; 7]);
        }
        ReadOnlySource::from(buffer)
    }

    #[test]
    fn test_compressed_int_stream() {
        let buffer = create_stream_buffer();
        let mut stream = CompressedIntStream::wrap(buffer);
        let mut block: [u32; COMPRESSION_BLOCK_SIZE] = [0u32; COMPRESSION_BLOCK_SIZE];

        stream.read(&mut block[0..2]);
        assert_eq!(block[0], 0);
        assert_eq!(block[1], 1);

        // reading does not consume the stream
        stream.read(&mut block[0..2]);
        assert_eq!(block[0], 0);
        assert_eq!(block[1], 1);
        stream.skip(2);

        stream.skip(5);
        stream.read(&mut block[0..3]);
        stream.skip(3);

        assert_eq!(block[0], 7);
        assert_eq!(block[1], 8);
        assert_eq!(block[2], 9);
        stream.skip(500);
        stream.read(&mut block[0..3]);
        stream.skip(3);

        assert_eq!(block[0], 510);
        assert_eq!(block[1], 511);
        assert_eq!(block[2], 512);
        stream.skip(511);
        stream.read(&mut block[..1]);
        assert_eq!(block[0], 1024);
    }
}
