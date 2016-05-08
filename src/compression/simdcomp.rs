use libc::size_t;
use super::NUM_DOCS_PER_BLOCK;

const COMPRESSED_BLOCK_MAX_SIZE: usize = NUM_DOCS_PER_BLOCK * 4 + 1; 

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


pub struct SIMDBlockEncoder {
    output: [u8; COMPRESSED_BLOCK_MAX_SIZE],
    output_len: usize,
}

impl SIMDBlockEncoder {
    
    pub fn new() -> SIMDBlockEncoder {
        SIMDBlockEncoder {
            output: [0u8; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
        }    
    }
    
    pub fn compress_block_sorted(&mut self, vals: &[u32], offset: u32) -> &[u8] {
        let compressed_size = unsafe { compress_sorted_cpp(vals.as_ptr(), self.output.as_mut_ptr(), offset) };
        &self.output[..compressed_size]
    }
    
    pub fn compress_block_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        let compressed_size = unsafe { compress_unsorted_cpp(vals.as_ptr(), self.output.as_mut_ptr()) };
        &self.output[..compressed_size]
    }
    
    pub fn compress_vint_sorted(&mut self, input: &[u32], mut offset: u32) -> &[u8] {
        let mut byte_written = 0;
        for v in input.iter() {
            let mut to_encode: u32 = *v - offset;
            offset = *v;
            loop {
                let next_byte: u8 = (to_encode % 128u32) as u8;
                to_encode /= 128u32;
                if to_encode == 0u32 {
                    self.output[byte_written] = next_byte | 128u8;
                    byte_written += 1;
                    break;
                }
                else {
                    self.output[byte_written] = next_byte;
                    byte_written += 1;
                }
            }
        }
        return &self.output[..byte_written];
    }
    
    pub fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8] {
        let mut byte_written = 0;
        for &i in input.iter() {
            let mut to_encode: u32 = i;
            loop {
                let next_byte: u8 = (to_encode % 128u32) as u8;
                to_encode /= 128u32;
                if to_encode == 0u32 {
                    self.output[byte_written] = next_byte | 128u8;
                    byte_written += 1;
                    break;
                }
                else {
                    self.output[byte_written] = next_byte;
                    byte_written += 1;
                }
            }
        }
        return &self.output[..byte_written];
    }
    
}

pub struct SIMDBlockDecoder {
    output: [u32; COMPRESSED_BLOCK_MAX_SIZE],
    output_len: usize,
}


impl SIMDBlockDecoder {
    pub fn new() -> SIMDBlockDecoder {
        SIMDBlockDecoder {
            output: [0u32; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
        }    
    }
    
    pub fn uncompress_block_sorted<'a>(&mut self, compressed_data: &'a [u8], offset: u32) -> &'a[u8] {
        let consumed_size = unsafe { uncompress_sorted_cpp(compressed_data.as_ptr(), self.output.as_mut_ptr(), offset) };
        self.output_len = NUM_DOCS_PER_BLOCK;
        &compressed_data[consumed_size..]
    }
    
    pub fn uncompress_block_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> &'a[u8] {
        let consumed_size = unsafe { uncompress_unsorted_cpp(compressed_data.as_ptr(), self.output.as_mut_ptr()) };
        self.output_len = NUM_DOCS_PER_BLOCK;
        &compressed_data[consumed_size..]
    }
    
    pub fn uncompress_vint_sorted<'a>(
        &mut self,
        compressed_data: &'a [u8],
        offset: u32,
        num_els: usize) -> &'a [u8] {
        let mut read_byte = 0;
        let mut result = offset;
        for i in 0..num_els {
            let mut shift = 0u32;
            loop {
                let cur_byte = compressed_data[read_byte];
                read_byte += 1;
                result += ((cur_byte % 128u8) as u32) << shift;
                if cur_byte & 128u8 != 0u8 {
                    break;
                }
                shift += 7;
            }
            self.output[i] = result;
        }
        self.output_len = num_els;
        &compressed_data[read_byte..]
    }
    
    pub fn uncompress_vint_unsorted<'a>(
        &mut self,
        compressed_data: &'a [u8],
        num_els: usize) -> &'a [u8] {
        let mut read_byte = 0;
        for i in 0..num_els {
            let mut result = 0u32;
            let mut shift = 0u32;
            loop {
                let cur_byte = compressed_data[read_byte];
                read_byte += 1;
                result += ((cur_byte % 128u8) as u32) << shift;
                if cur_byte & 128u8 != 0u8 {
                    break;
                }
                shift += 7;
            }
            self.output[i] = result;
        }
        self.output_len = num_els;
        &compressed_data[read_byte..]
    }
    
    pub fn output(&self,) -> &[u32] {
        &self.output[..self.output_len]
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use compression::NUM_DOCS_PER_BLOCK;
    use compression::tests::generate_array;

    #[test]
    fn test_encode_sorted_block() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| i*7).collect();
        let mut encoder = SIMDBlockEncoder::new();
        let compressed_data = encoder.compress_block_sorted(&vals, 0);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(compressed_data, 0);    
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
        let compressed_data = encoder.compress_block_sorted(&vals, 10);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(compressed_data, 10);    
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
        let compressed_data = encoder.compress_block_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(&compressed, 10);    
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
        let compressed_data = encoder.compress_block_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = SIMDBlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(&compressed, 10);    
            assert_eq!(remaining_data.len(), 1);
            assert_eq!(remaining_data[0], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output()[i]);
        }
    }
    
    
    #[test]
    fn test_encode_vint() {
        {
            let expected_length = 123;
            let mut encoder = SIMDBlockEncoder::new();
            let input: Vec<u32> = (0u32..123u32)
                .map(|i| 4 + i * 7 / 2)
                .into_iter()
                .collect();
            for offset in [0u32, 1u32, 2u32].iter() {
                let encoded_data = encoder.compress_vint_sorted(&input, *offset);
                assert_eq!(encoded_data.len(), expected_length);
                let mut decoder = SIMDBlockDecoder::new();
                let remaining_data = decoder.uncompress_vint_sorted(&encoded_data, *offset, input.len());
                assert_eq!(0, remaining_data.len());
                for (&decoded, &expected) in decoder.output().iter().zip(input.iter()) {
                    assert_eq!(decoded, expected);
                }
            }
        }
        {
            let mut encoder = SIMDBlockEncoder::new();
            let input = vec!(3u32, 17u32, 187u32);
            let encoded_data = encoder.compress_vint_sorted(&input, 0);
            assert_eq!(encoded_data.len(), 4);
            assert_eq!(encoded_data[0], 3u8 + 128u8);
            assert_eq!(encoded_data[1], (17u8 - 3u8) + 128u8);
            assert_eq!(encoded_data[2], (187u8 - 17u8 - 128u8));
            assert_eq!(encoded_data[3], (1u8 + 128u8));
        }
    }


    #[bench]
    fn bench_compress(b: &mut Bencher) {
        let mut encoder = SIMDBlockEncoder::new();
        let data = generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        b.iter(|| {
            encoder.compress_block_sorted(&data, 0u32);
        });
    }
    
    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = SIMDBlockEncoder::new();
        let data = generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        let compressed = encoder.compress_block_sorted(&data, 0u32);
        let mut decoder = SIMDBlockDecoder::new(); 
        b.iter(|| {
            decoder.uncompress_block_sorted(compressed, 0u32);
        });
    }

}



    