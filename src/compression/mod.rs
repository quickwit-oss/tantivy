#![allow(dead_code)]


mod composite;
pub use self::composite::{CompositeEncoder, CompositeDecoder};

#[cfg(feature="simdcompression")]
mod compression_simd;
#[cfg(feature="simdcompression")]
pub use self::compression_simd::{BlockEncoder, BlockDecoder};


#[cfg(not(feature="simdcompression"))]
mod compression_nosimd;
#[cfg(not(feature="simdcompression"))]
pub use self::compression_nosimd::{BlockEncoder, BlockDecoder};


pub trait VIntEncoder {
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8];
    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8];
}

pub trait VIntDecoder {
    fn uncompress_vint_sorted<'a>(&mut self, compressed_data: &'a [u8], offset: u32, num_els: usize) -> &'a [u8];
    fn uncompress_vint_unsorted<'a>(&mut self, compressed_data: &'a [u8], num_els: usize) -> &'a [u8];
}

impl VIntEncoder for BlockEncoder{
    
    fn compress_vint_sorted(&mut self, input: &[u32], mut offset: u32) -> &[u8] {
        let mut byte_written = 0;
        for &v in input {
            let mut to_encode: u32 = v - offset;
            offset = v;
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
        &self.output[..byte_written]
    }
    
    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8] {
        let mut byte_written = 0;
        for &v in input {
            let mut to_encode: u32 = v;
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
        &self.output[..byte_written]
    }
} 

impl VIntDecoder for BlockDecoder {
    
    fn uncompress_vint_sorted<'a>(
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
    
    fn uncompress_vint_unsorted<'a>(
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
    
}

    
    

pub const NUM_DOCS_PER_BLOCK: usize = 128; //< should be a power of 2 to let the compiler optimize.

#[cfg(test)]
pub mod tests {

    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;
    use super::*;
    use test::Bencher;
    
    fn generate_array_with_seed(n: usize, ratio: f32, seed_val: u32) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, seed_val];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        (0..u32::max_value())
            .filter(|_| rng.next_f32()< ratio)
            .take(n)
            .collect()
    }

    pub fn generate_array(n: usize, ratio: f32) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }

    #[test]
    fn test_encode_sorted_block() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| i*7).collect();
        let mut encoder = BlockEncoder::new();
        let compressed_data = encoder.compress_block_sorted(&vals, 0);
        let mut decoder = BlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(compressed_data, 0);    
            assert_eq!(remaining_data.len(), 0);
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output(i));
        }
    }

    #[test]
    fn test_encode_sorted_block_with_offset() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| 11 + i*7).collect();
        let mut encoder = BlockEncoder::new();
        let compressed_data = encoder.compress_block_sorted(&vals, 10);
        let mut decoder = BlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(compressed_data, 10);    
            assert_eq!(remaining_data.len(), 0);
        }
        for i in 0..128 {
            assert_eq!(vals[i], decoder.output(i));
        }
    }
    
    #[test]
    fn test_encode_sorted_block_with_junk() {
        let mut compressed: Vec<u8> = Vec::new();
        let n = 128;
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32)*7u32).collect();
        let mut encoder = BlockEncoder::new();
        let compressed_data = encoder.compress_block_sorted(&vals, 10);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = BlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_sorted(&compressed, 10);    
            assert_eq!(remaining_data.len(), 1);
            assert_eq!(remaining_data[0], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output(i));
        }
    }

    #[test]
    fn test_encode_unsorted_block_with_junk() {
        let mut compressed: Vec<u8> = Vec::new();
        let n = 128;
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32)*7u32 % 12).collect();
        let mut encoder = BlockEncoder::new();
        let compressed_data = encoder.compress_block_unsorted(&vals);
        compressed.extend_from_slice(compressed_data);
        compressed.push(173u8);
        let mut decoder = BlockDecoder::new();
        {
            let remaining_data = decoder.uncompress_block_unsorted(&compressed);    
            assert_eq!(remaining_data.len(), 1);
            assert_eq!(remaining_data[0], 173u8);
        }
        for i in 0..n {
            assert_eq!(vals[i], decoder.output(i));
        }
    }
    
    
    #[test]
    fn test_encode_vint() {
        {
            let expected_length = 123;
            let mut encoder = BlockEncoder::new();
            let input: Vec<u32> = (0u32..123u32)
                .map(|i| 4 + i * 7 / 2)
                .into_iter()
                .collect();
            for offset in &[0u32, 1u32, 2u32] {
                let encoded_data = encoder.compress_vint_sorted(&input, *offset);
                assert_eq!(encoded_data.len(), expected_length);
                let mut decoder = BlockDecoder::new();
                let remaining_data = decoder.uncompress_vint_sorted(&encoded_data, *offset, input.len());
                assert_eq!(0, remaining_data.len());
                assert_eq!(input, decoder.output_array());
            }
        }
        {
            let mut encoder = BlockEncoder::new();
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
        let mut encoder = BlockEncoder::new();
        let data = generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        b.iter(|| {
            encoder.compress_block_sorted(&data, 0u32);
        });
    }
    
    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        let compressed = encoder.compress_block_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::new(); 
        b.iter(|| {
            decoder.uncompress_block_sorted(compressed, 0u32);
        });
    }

}
