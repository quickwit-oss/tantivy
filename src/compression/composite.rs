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
            output: Vec::new(),
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
            vals: Vec::new(),
        }    
    }
    
    pub fn uncompress_sorted(&mut self, mut compressed_data: &[u8], doc_freq: usize) -> &[u32] {
        let mut offset = 0u32;
        self.vals.clear();
        let num_blocks = doc_freq / NUM_DOCS_PER_BLOCK;
        for _ in 0..num_blocks {
            compressed_data = self.block_decoder.uncompress_block_sorted(compressed_data, offset);
            offset = self.block_decoder.output()[NUM_DOCS_PER_BLOCK - 1];
            self.vals.extend_from_slice(self.block_decoder.output());
        }
        self.block_decoder.uncompress_vint_sorted(compressed_data, offset, doc_freq % NUM_DOCS_PER_BLOCK);
        self.vals.extend_from_slice(self.block_decoder.output());
        &self.vals
    }
    
    pub fn uncompress_unsorted(&mut self, mut compressed_data: &[u8], doc_freq: usize) -> &[u32] {
        self.vals.clear();
        let num_blocks = doc_freq / NUM_DOCS_PER_BLOCK;
        for _ in 0..num_blocks {
            compressed_data = self.block_decoder.uncompress_block_unsorted(compressed_data);
            self.vals.extend_from_slice(self.block_decoder.output());
        }
        self.block_decoder.uncompress_vint_unsorted(compressed_data, doc_freq % NUM_DOCS_PER_BLOCK);
        self.vals.extend_from_slice(self.block_decoder.output());
        &self.vals
    }
}
