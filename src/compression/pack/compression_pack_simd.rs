use super::super::COMPRESSION_BLOCK_SIZE;

const COMPRESSED_BLOCK_MAX_SIZE: usize = COMPRESSION_BLOCK_SIZE * 4 + 1;

mod simdcomp {
    use libc::size_t;

    extern "C" {
        pub fn compress_sorted(data: *const u32, output: *mut u8, offset: u32) -> size_t;

        pub fn uncompress_sorted(
            compressed_data: *const u8,
            output: *mut u32,
            offset: u32,
        ) -> size_t;

        pub fn compress_unsorted(data: *const u32, output: *mut u8) -> size_t;

        pub fn uncompress_unsorted(compressed_data: *const u8, output: *mut u32) -> size_t;
    }
}

fn compress_sorted(vals: &[u32], output: &mut [u8], offset: u32) -> usize {
    unsafe { simdcomp::compress_sorted(vals.as_ptr(), output.as_mut_ptr(), offset) }
}

fn uncompress_sorted(compressed_data: &[u8], output: &mut [u32], offset: u32) -> usize {
    unsafe { simdcomp::uncompress_sorted(compressed_data.as_ptr(), output.as_mut_ptr(), offset) }
}

fn compress_unsorted(vals: &[u32], output: &mut [u8]) -> usize {
    unsafe { simdcomp::compress_unsorted(vals.as_ptr(), output.as_mut_ptr()) }
}

fn uncompress_unsorted(compressed_data: &[u8], output: &mut [u32]) -> usize {
    unsafe { simdcomp::uncompress_unsorted(compressed_data.as_ptr(), output.as_mut_ptr()) }
}

pub struct BlockEncoder {
    pub output: [u8; COMPRESSED_BLOCK_MAX_SIZE],
    pub output_len: usize,
}

impl BlockEncoder {
    pub fn new() -> BlockEncoder {
        BlockEncoder {
            output: [0u8; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
        }
    }

    pub fn compress_block_sorted(&mut self, vals: &[u32], offset: u32) -> &[u8] {
        let compressed_size = compress_sorted(vals, &mut self.output, offset);
        &self.output[..compressed_size]
    }

    pub fn compress_block_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        let compressed_size = compress_unsorted(vals, &mut self.output);
        &self.output[..compressed_size]
    }
}

pub struct BlockDecoder {
    pub output: [u32; COMPRESSED_BLOCK_MAX_SIZE],
    pub output_len: usize,
}

impl BlockDecoder {
    pub fn new() -> BlockDecoder {
        BlockDecoder::with_val(0u32)
    }

    pub fn with_val(val: u32) -> BlockDecoder {
        BlockDecoder {
            output: [val; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
        }
    }

    pub fn uncompress_block_sorted(&mut self, compressed_data: &[u8], offset: u32) -> usize {
        let consumed_size = uncompress_sorted(compressed_data, &mut self.output, offset);
        self.output_len = COMPRESSION_BLOCK_SIZE;
        consumed_size
    }

    pub fn uncompress_block_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> usize {
        let consumed_size = uncompress_unsorted(compressed_data, &mut self.output);
        self.output_len = COMPRESSION_BLOCK_SIZE;
        consumed_size
    }

    #[inline]
    pub fn output_array(&self) -> &[u32] {
        &self.output[..self.output_len]
    }

    #[inline]
    pub fn output(&self, idx: usize) -> u32 {
        self.output[idx]
    }
}

#[cfg(test)]
mod tests {

    use super::BlockEncoder;

    #[test]
    fn test_all_docs_compression_len() {
        let data: Vec<u32> = (0u32..128u32).collect();
        let mut encoder = BlockEncoder::new();
        let compressed = encoder.compress_block_sorted(&data, 0u32);
        assert_eq!(compressed.len(), 17);
    }

}
