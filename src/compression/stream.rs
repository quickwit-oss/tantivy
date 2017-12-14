use compression::BlockDecoder;
use compression::COMPRESSION_BLOCK_SIZE;
use compression::compressed_block_size;
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
    inner_offset: usize,
}

impl CompressedIntStream {
    /// Opens a compressed int stream.
    pub(crate) fn wrap(source: ReadOnlySource) -> CompressedIntStream {
        CompressedIntStream {
            buffer: SourceRead::from(source),
            block_decoder: BlockDecoder::new(),
            inner_offset: COMPRESSION_BLOCK_SIZE,
        }
    }

    /// Fills a buffer with the next `output.len()` integers,
    /// and advance the stream by that many els.
    pub fn read(&mut self, output: &mut [u32]) {
        let mut num_els: usize = output.len();
        let mut start: usize = 0;
        loop {
            let available = COMPRESSION_BLOCK_SIZE - self.inner_offset;
            if num_els >= available {
                if available > 0 {
                    let uncompressed_block = &self.block_decoder.output_array()
                        [self.inner_offset..];
                    output[start..][..available].clone_from_slice(uncompressed_block);
                }
                num_els -= available;
                start += available;
                let num_consumed_bytes = self.block_decoder.uncompress_block_unsorted(
                    self.buffer.as_ref(),
                );
                self.buffer.advance(num_consumed_bytes);
                self.inner_offset = 0;
            } else {
                let uncompressed_block = &self.block_decoder.output_array()[self.inner_offset..
                                                                                self.inner_offset +
                                                                                    num_els];
                output[start..][..num_els].clone_from_slice(uncompressed_block);
                self.inner_offset += num_els;
                break;
            }
        }
    }


    /// Skip the next `skip_len` integer.
    ///
    /// If a full block is skipped, calling
    /// `.skip(...)` will avoid decompressing it.
    pub fn skip(&mut self, mut skip_len: usize) {
        let available = COMPRESSION_BLOCK_SIZE - self.inner_offset;
        if available >= skip_len {
            self.inner_offset += skip_len;
        } else {
            skip_len -= available;
            // entirely skip decompressing some blocks.
            while skip_len >= COMPRESSION_BLOCK_SIZE {
                skip_len -= COMPRESSION_BLOCK_SIZE;
                let num_bits: u8 = self.buffer.as_ref()[0];
                let block_len = compressed_block_size(num_bits);
                self.buffer.advance(block_len);
            }
            let num_consumed_bytes = self.block_decoder.uncompress_block_unsorted(
                self.buffer.as_ref(),
            );
            self.buffer.advance(num_consumed_bytes);
            self.inner_offset = skip_len;
        }
    }
}


#[cfg(test)]
pub mod tests {

    use super::CompressedIntStream;
    use compression::compressed_block_size;
    use compression::COMPRESSION_BLOCK_SIZE;
    use compression::BlockEncoder;
    use directory::ReadOnlySource;

    fn create_stream_buffer() -> ReadOnlySource {
        let mut buffer: Vec<u8> = vec![];
        let mut encoder = BlockEncoder::new();
        let vals: Vec<u32> = (0u32..1_025u32).collect();
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
        stream.skip(5);
        stream.read(&mut block[0..3]);
        assert_eq!(block[0], 7);
        assert_eq!(block[1], 8);
        assert_eq!(block[2], 9);
        stream.skip(500);
        stream.read(&mut block[0..3]);
        assert_eq!(block[0], 510);
        assert_eq!(block[1], 511);
        assert_eq!(block[2], 512);
        stream.skip(511);
        stream.read(&mut block[..1]);
        assert_eq!(block[0], 1024);
    }
}
