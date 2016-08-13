use compression::SIMDBlockEncoder;
use compression::SIMDBlockDecoder;
use super::NUM_DOCS_PER_BLOCK;

pub struct CompositeEncoder {
    block_encoder: SIMDBlockEncoder,
    output: Vec<u8>,
}

impl CompositeEncoder {
    
    pub fn new() -> CompositeEncoder {
        CompositeEncoder {
            block_encoder: SIMDBlockEncoder::new(),
            output: Vec::with_capacity(500_000),
        }
    }
    
    pub fn compress_sorted(&mut self, vals: &[u32]) -> &[u8] {
        self.output.clear();
        let num_blocks = vals.len() / NUM_DOCS_PER_BLOCK;
        let mut offset = 0u32;
        for i in 0..num_blocks {
            let vals_slice = &vals[i * NUM_DOCS_PER_BLOCK .. (i + 1) * NUM_DOCS_PER_BLOCK];
            let block_compressed = self.block_encoder.compress_block_sorted(&vals_slice, offset);
            offset = vals_slice[NUM_DOCS_PER_BLOCK - 1];
            self.output.extend_from_slice(block_compressed);
        }
        let vint_compressed = self.block_encoder.compress_vint_sorted(&vals[num_blocks * NUM_DOCS_PER_BLOCK..], offset);
        self.output.extend_from_slice(vint_compressed);
        &self.output
    }
    
    pub fn compress_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        self.output.clear();
        let num_blocks = vals.len() / NUM_DOCS_PER_BLOCK;
        for i in 0..num_blocks {
            let vals_slice = &vals[i * NUM_DOCS_PER_BLOCK .. (i + 1) * NUM_DOCS_PER_BLOCK];
            let block_compressed = self.block_encoder.compress_block_unsorted(&vals_slice);
            self.output.extend_from_slice(block_compressed);
        }
        let vint_compressed = self.block_encoder.compress_vint_unsorted(&vals[num_blocks * NUM_DOCS_PER_BLOCK..]);
        self.output.extend_from_slice(vint_compressed);
        &self.output
    }
}


pub struct CompositeDecoder {
    block_decoder: SIMDBlockDecoder,
    vals: Vec<u32>,
}


impl CompositeDecoder {
    pub fn new() -> CompositeDecoder {
        CompositeDecoder {
            block_decoder: SIMDBlockDecoder::new(),
            vals: Vec::with_capacity(500_000),
        }    
    }
    
    pub fn uncompress_sorted(&mut self, mut compressed_data: &[u8], uncompressed_len: usize) -> &[u32] {
        if uncompressed_len > self.vals.capacity() {
            let extra_capacity = uncompressed_len - self.vals.capacity();
            self.vals.reserve(extra_capacity);
        }
        let mut offset = 0u32;
        self.vals.clear();
        let num_blocks = uncompressed_len / NUM_DOCS_PER_BLOCK;
        for _ in 0..num_blocks {
            compressed_data = self.block_decoder.uncompress_block_sorted(compressed_data, offset);
            offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
            self.vals.extend_from_slice(self.block_decoder.output_array());
        }
        self.block_decoder.uncompress_vint_sorted(compressed_data, offset, uncompressed_len % NUM_DOCS_PER_BLOCK);
        self.vals.extend_from_slice(self.block_decoder.output_array());
        &self.vals
    }
    
    pub fn uncompress_unsorted(&mut self, mut compressed_data: &[u8], uncompressed_len: usize) -> &[u32] {
        self.vals.clear();
        let num_blocks = uncompressed_len / NUM_DOCS_PER_BLOCK;
        for _ in 0..num_blocks {
            compressed_data = self.block_decoder.uncompress_block_unsorted(compressed_data);
            self.vals.extend_from_slice(self.block_decoder.output_array());
        }
        self.block_decoder.uncompress_vint_unsorted(compressed_data, uncompressed_len % NUM_DOCS_PER_BLOCK);
        self.vals.extend_from_slice(self.block_decoder.output_array());
        &self.vals
    }
}

impl Into<Vec<u32>> for CompositeDecoder {
    fn into(self) -> Vec<u32> {
        self.vals
    }
}


#[cfg(test)]
pub mod tests {
    
    use test::Bencher;
    use super::*;
    use compression::tests::generate_array;

    #[test]
    fn test_composite_unsorted() {
        let data = generate_array(10_000, 0.1);
        let mut encoder = CompositeEncoder::new();
        let compressed = encoder.compress_unsorted(&data);
        assert_eq!(compressed.len(), 19_790);
        let mut decoder = CompositeDecoder::new();
        let result = decoder.uncompress_unsorted(&compressed, data.len());
        for i in 0..data.len() {
            assert_eq!(data[i], result[i]);
        } 
    }

    #[test]
    fn test_composite_sorted() {
        let data = generate_array(10_000, 0.1);
        let mut encoder = CompositeEncoder::new();
        let compressed = encoder.compress_sorted(&data);
        assert_eq!(compressed.len(), 7_822);
        let mut decoder = CompositeDecoder::new();
        let result = decoder.uncompress_sorted(&compressed, data.len());
        for i in 0..data.len() {
            assert_eq!(data[i], result[i]);
        } 
    }
    
    
    const BENCH_NUM_INTS: usize = 99_968;
    
    #[bench]
    fn bench_compress(b: &mut Bencher) {
        let mut encoder = CompositeEncoder::new();
        let data = generate_array(BENCH_NUM_INTS, 0.1);
        b.iter(|| {
            encoder.compress_sorted(&data);
        });
    }
    
    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = CompositeEncoder::new();
        let data = generate_array(BENCH_NUM_INTS, 0.1);
        let compressed = encoder.compress_sorted(&data);
        let mut decoder = CompositeDecoder::new(); 
        b.iter(|| {
            decoder.uncompress_sorted(compressed, BENCH_NUM_INTS);
        });
    }
}



