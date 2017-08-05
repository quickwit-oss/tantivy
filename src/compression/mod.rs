#![allow(dead_code)]


mod stream;

pub use self::stream::CompressedIntStream;

#[cfg(not(feature="simdcompression"))]
mod pack {
    mod compression_pack_nosimd;
    pub use self::compression_pack_nosimd::*;
}

#[cfg(feature="simdcompression")]
mod pack {
    mod compression_pack_simd;
    pub use self::compression_pack_simd::*;
}

pub use self::pack::{BlockEncoder, BlockDecoder, compressedbytes};

#[cfg( any(not(feature="simdcompression"), target_env="msvc") )]
mod vint {
    mod compression_vint_nosimd;
    pub use self::compression_vint_nosimd::*;
}

#[cfg( all(feature="simdcompression", not(target_env="msvc")) )]
mod vint {
    mod compression_vint_simd;
    pub use self::compression_vint_simd::*;
}


pub trait VIntEncoder {
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8];
    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8];
}

pub trait VIntDecoder {
    fn uncompress_vint_sorted<'a>(&mut self,
                                  compressed_data: &'a [u8],
                                  offset: u32,
                                  num_els: usize)
                                  -> &'a [u8];
    fn uncompress_vint_unsorted<'a>(&mut self,
                                    compressed_data: &'a [u8],
                                    num_els: usize)
                                    -> &'a [u8];
}

impl VIntEncoder for BlockEncoder {
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8] {
        vint::compress_sorted(input, &mut self.output, offset)
    }

    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8] {
        vint::compress_unsorted(input, &mut self.output)
    }
}

impl VIntDecoder for BlockDecoder {
    fn uncompress_vint_sorted<'a>(&mut self,
                                  compressed_data: &'a [u8],
                                  offset: u32,
                                  num_els: usize)
                                  -> &'a [u8] {
        self.output_len = num_els;
        vint::uncompress_sorted(compressed_data, &mut self.output[..num_els], offset)
    }

    fn uncompress_vint_unsorted<'a>(&mut self,
                                    compressed_data: &'a [u8],
                                    num_els: usize)
                                    -> &'a [u8] {
        self.output_len = num_els;
        vint::uncompress_unsorted(compressed_data, &mut self.output[..num_els])
    }
}


pub const NUM_DOCS_PER_BLOCK: usize = 128; //< should be a power of 2 to let the compiler optimize.

#[cfg(test)]
pub mod tests {

    use super::*;
    use tests;
    use test::Bencher;

    #[test]
    fn test_encode_sorted_block() {
        let vals: Vec<u32> = (0u32..128u32).map(|i| i * 7).collect();
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
        let vals: Vec<u32> = (0u32..128u32).map(|i| 11 + i * 7).collect();
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
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32) * 7u32).collect();
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
        let vals: Vec<u32> = (0..n).map(|i| 11u32 + (i as u32) * 7u32 % 12).collect();
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
            let expected_length = 154;
            let mut encoder = BlockEncoder::new();
            let input: Vec<u32> = (0u32..123u32).map(|i| 4 + i * 7 / 2).into_iter().collect();
            for offset in &[0u32, 1u32, 2u32] {
                let encoded_data = encoder.compress_vint_sorted(&input, *offset);
                assert!(encoded_data.len() <= expected_length);
                let mut decoder = BlockDecoder::new();
                let remaining_data =
                    decoder.uncompress_vint_sorted(&encoded_data, *offset, input.len());
                assert_eq!(0, remaining_data.len());
                assert_eq!(input, decoder.output_array());
            }
        }
    }


    #[bench]
    fn bench_compress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        b.iter(|| { encoder.compress_block_sorted(&data, 0u32); });
    }

    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_DOCS_PER_BLOCK, 0.1);
        let compressed = encoder.compress_block_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::new();
        b.iter(|| { decoder.uncompress_block_sorted(compressed, 0u32); });
    }


    const NUM_INTS_BENCH_VINT: usize = 10;

    #[bench]
    fn bench_compress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_INTS_BENCH_VINT, 0.001);
        b.iter(|| { encoder.compress_vint_sorted(&data, 0u32); });
    }

    #[bench]
    fn bench_uncompress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_INTS_BENCH_VINT, 0.001);
        let compressed = encoder.compress_vint_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::new();
        b.iter(|| { decoder.uncompress_vint_sorted(compressed, 0u32, NUM_INTS_BENCH_VINT); });
    }

}
