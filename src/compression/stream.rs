use compression::BlockDecoder;
use compression::NUM_DOCS_PER_BLOCK;
use compression::compressedbytes;

pub struct CompressedIntStream<'a> {
    buffer: &'a [u8],
    block_decoder: BlockDecoder,
    inner_offset: usize,
}

impl<'a> CompressedIntStream<'a> {
    pub fn wrap(buffer: &'a [u8]) -> CompressedIntStream<'a> {
        CompressedIntStream {
            buffer: buffer,
            block_decoder: BlockDecoder::new(),
            inner_offset: NUM_DOCS_PER_BLOCK,
        }
    }

    pub fn read(&mut self, mut output: &mut [u32]) {
        let mut num_els: usize = output.len();
        let mut start: usize = 0;
        loop {
            let available = NUM_DOCS_PER_BLOCK - self.inner_offset;
            if num_els >= available {
                if available > 0 {
                    let uncompressed_block = &self.block_decoder.output_array()[self.inner_offset..];
                    &mut output[start..start + available].clone_from_slice(uncompressed_block);
                }
                num_els -= available;
                start += available;
                self.buffer = self.block_decoder.uncompress_block_unsorted(self.buffer);
                self.inner_offset = 0;
            }
            else {
                let uncompressed_block = &self.block_decoder.output_array()[self.inner_offset..self.inner_offset + num_els];
                &output[start..start + num_els].clone_from_slice(uncompressed_block);
                self.inner_offset += num_els;
                break;
            }
        }
    }

    pub fn skip(&mut self, mut skip_len: usize) {
        let available = NUM_DOCS_PER_BLOCK - self.inner_offset;
        if available >= skip_len {
            self.inner_offset += skip_len;
        }
        else {
            skip_len -= available;
            // entirely skip decompressing some blocks.
            while skip_len >= NUM_DOCS_PER_BLOCK {
                skip_len -= NUM_DOCS_PER_BLOCK;
                let num_bits: u8 = self.buffer[0];
                let block_len = compressedbytes(128, num_bits);
                self.buffer = &self.buffer[1 + block_len..];
            }
            self.buffer = self.block_decoder.uncompress_block_unsorted(self.buffer);
            self.inner_offset = skip_len;
        }
    }
}


#[cfg(test)]
pub mod tests {

    use super::CompressedIntStream;
    use tests;
    use compression::compressedbytes;
    use compression::NUM_DOCS_PER_BLOCK;
    use compression::BlockEncoder;

    fn create_stream_buffer() -> Vec<u8> {
        let mut buffer: Vec<u8> = vec!();
        let mut encoder = BlockEncoder::new();
        let vals: Vec<u32> = (0u32..1_025u32).collect();
        for chunk in vals.chunks(NUM_DOCS_PER_BLOCK) {
            let compressed_block = encoder.compress_block_unsorted(chunk);
            let num_bits = compressed_block[0];
            assert_eq!(compressedbytes(128, num_bits) + 1, compressed_block.len());
            buffer.extend_from_slice(compressed_block);
        }
        buffer
    }

    #[test]
    fn test_compressed_int_stream() {
        let buffer = create_stream_buffer();
        let mut stream = CompressedIntStream::wrap(&buffer[..]);
        let mut block: [u32; NUM_DOCS_PER_BLOCK] = [0u32; NUM_DOCS_PER_BLOCK];

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
