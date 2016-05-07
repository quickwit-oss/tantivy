use libc::size_t;

extern {
    // complete s4-bp128-dm
    fn compress_sorted_cpp(
        data: *const u32,
        output: *mut u8,
        offset: u32) -> size_t;
   
    fn uncompress_sorted_cpp(
        compressed_data: *const u8,
        output: *mut u32,
        offset: u32) -> size_t;
        
    fn compress_unsorted_cpp(
        data: *const u32,
        output: *mut u8) -> size_t;
   
    fn uncompress_unsorted_cpp(
        compressed_data: *const u8,
        output: *mut u32) -> size_t;
}

const BLOCK_SIZE: usize = 128;
const COMPRESSED_BLOCK_MAX_SIZE: usize = BLOCK_SIZE * 4 + 1; 

pub struct SIMDBlockEncoder {
    output_buffer: [u8; COMPRESSED_BLOCK_MAX_SIZE],
}

impl SIMDBlockEncoder {
    
    pub fn new() -> SIMDBlockEncoder {
        SIMDBlockEncoder {
            output_buffer: [0u8; COMPRESSED_BLOCK_MAX_SIZE]
        }    
    }
    
    pub fn compress_sorted(&mut self, vals: &[u32], offset: u32) -> &[u8] {
        let compressed_size = unsafe { compress_sorted_cpp(vals.as_ptr(), self.output_buffer.as_mut_ptr(), offset) };
        &self.output_buffer[..compressed_size]
    }
    
    pub fn compress_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        let compressed_size = unsafe { compress_unsorted_cpp(vals.as_ptr(), self.output_buffer.as_mut_ptr()) };
        &self.output_buffer[..compressed_size]
    }
}

pub struct SIMDBlockDecoder {
    output_buffer: [u32; COMPRESSED_BLOCK_MAX_SIZE],
}


impl SIMDBlockDecoder {
    pub fn new() -> SIMDBlockDecoder {
        SIMDBlockDecoder {
            output_buffer: [0u32; COMPRESSED_BLOCK_MAX_SIZE]
        }    
    }
    
    pub fn uncompress_sorted<'a>(&mut self, compressed_data: &'a [u8], offset: u32) -> &'a[u8] {
        let consumed_size = unsafe { uncompress_sorted_cpp(compressed_data.as_ptr(), self.output_buffer.as_mut_ptr(), offset) };
        &compressed_data[consumed_size..]
    }
    
    pub fn uncompress_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> &'a[u8] {
        let consumed_size = unsafe { uncompress_unsorted_cpp(compressed_data.as_ptr(), self.output_buffer.as_mut_ptr()) };
        &compressed_data[consumed_size..]
    }
    
    pub fn output(&self,) -> &[u32] {
        &self.output_buffer
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_encode_sorted_block() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| i*7).collect();
        let mut encoder = SIMDBlockEncoder::new();
        let compressed_data = encoder.compress_sorted(&vals, 0);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_sorted(compressed_data, 0);    
            assert_eq!(remaining_data.len(), 0);
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output()[i]);
        }
    }

    #[test]
    fn test_encode_sorted_block_with_offset() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| 11 + i*7).collect();
        let mut encoder = SIMDBlockEncoder::new();
        let compressed_data = encoder.compress_sorted(&vals, 10);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_sorted(compressed_data, 10);    
            assert_eq!(remaining_data.len(), 0);
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output()[i]);
        }
    }
    
    #[test]
    fn test_encode_sorted_block_with_junk() {
        let mut compressed: Vec<u8> = Vec::new();
        let n = 128;
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32)*7u32).collect();
        let mut encoder = SIMDBlockEncoder::new();
        let compressed_data = encoder.compress_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_sorted(&compressed, 10);    
            assert_eq!(remaining_data.len(), 1);
            assert_eq!(remaining_data[0], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output()[i]);
        }
    }

    #[test]
    fn test_encode_unsorted_block_with_junk() {
        let mut compressed: Vec<u8> = Vec::new();
        let n = 128;
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32)*7u32 % 12).collect();
        let mut encoder = SIMDBlockEncoder::new();
        let compressed_data = encoder.compress_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_sorted(&compressed, 10);    
            assert_eq!(remaining_data.len(), 1);
            assert_eq!(remaining_data[0], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output()[i]);
        }
    }
}
