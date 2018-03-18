#![allow(dead_code)]


mod stream;

pub const COMPRESSION_BLOCK_SIZE: usize = 128;
const COMPRESSED_BLOCK_MAX_SIZE: usize = COMPRESSION_BLOCK_SIZE * 4 + 1;

pub use self::stream::CompressedIntStream;
use std::cmp;


use bitpacking::BitPacker;

#[cfg(not(feature = "simdcompression"))]
pub use bitpacking::ScalarBitPacker as BitPackerImpl;
#[cfg(not(feature = "simdcompression"))]
const MINI_BLOCK: usize = 4;

#[cfg(feature = "simdcompression")]
pub use bitpacking::SSE3BitPacker as BitPackerImpl;
#[cfg(feature = "simdcompression")]
const MINI_BLOCK: usize = 1;

/// Returns the size in bytes of a compressed block, given `num_bits`.
pub fn compressed_block_size(num_bits: u8) -> usize {
    1 + (num_bits as usize) * BitPackerImpl::BLOCK_LEN / 8
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
        assert_eq!(vals.len(), COMPRESSION_BLOCK_SIZE);
        let mut num_bits = 0;
        let mut offsets = [offset; MINI_BLOCK];
        for i in 1..MINI_BLOCK {
            offsets[i] = vals[(i * BitPackerImpl::BLOCK_LEN) - 1];
        }
        for i in 0..MINI_BLOCK {
            let block = &vals[i * BitPackerImpl::BLOCK_LEN.. (i + 1)*BitPackerImpl::BLOCK_LEN];
            num_bits = cmp::max(BitPackerImpl::num_bits_sorted(offsets[i], block), num_bits);
        }
        self.output[0] = num_bits;
        let compressed_chunk_len = (num_bits as usize)  * BitPackerImpl::BLOCK_LEN / 8;
        let mut written_size = 1;
        for i in 0..MINI_BLOCK {
            let block = &vals[i * BitPackerImpl::BLOCK_LEN.. (i + 1)*BitPackerImpl::BLOCK_LEN];
            BitPackerImpl::compress_sorted(offsets[i], block, &mut self.output[written_size..], num_bits);
            written_size += compressed_chunk_len;
        }
        &self.output[..written_size]
    }

    pub fn compress_block_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        assert_eq!(vals.len(), COMPRESSION_BLOCK_SIZE);
        let num_bits = vals.chunks(BitPackerImpl::BLOCK_LEN)
            .map(|chunk| BitPackerImpl::num_bits(chunk))
            .max()
            .unwrap_or(0u8);
        self.output[0] = num_bits;
        let mut written_size = 1;
        let compressed_chunk_len = (num_bits as usize)  * BitPackerImpl::BLOCK_LEN / 8;
        for chunk in vals.chunks(BitPackerImpl::BLOCK_LEN) {
            BitPackerImpl::compress(chunk, &mut self.output[written_size..], num_bits);
            written_size += compressed_chunk_len;
        }
        &self.output[..written_size]
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
    
    pub fn uncompress_block_sorted(&mut self, compressed_data: &[u8], mut offset: u32) -> usize {
        let num_bits = compressed_data[0];
        let mut read_size: usize = 1;
        let chunk_size: usize = (num_bits as usize) * BitPackerImpl::BLOCK_LEN / 8;
        for i in 0..MINI_BLOCK {
            BitPackerImpl::decompress_sorted(offset, &compressed_data[read_size..], &mut self.output[i*BitPackerImpl::BLOCK_LEN..], num_bits);
            offset = self.output[(i + 1)*BitPackerImpl::BLOCK_LEN - 1];
            read_size += chunk_size;
        }
        self.output_len = COMPRESSION_BLOCK_SIZE;
        read_size
    }

    pub fn uncompress_block_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> usize {
        let num_bits = compressed_data[0];
        let mut read_size: usize = 1;
        let chunk_size: usize = (num_bits as usize) * BitPackerImpl::BLOCK_LEN / 8;
        for i in 0..MINI_BLOCK {
            BitPackerImpl::decompress(&compressed_data[read_size..], &mut self.output[i*BitPackerImpl::BLOCK_LEN..], num_bits);
            read_size += chunk_size;
        }
        self.output_len = COMPRESSION_BLOCK_SIZE;
        read_size
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

mod vint;

pub trait VIntEncoder {
    /// Compresses an array of `u32` integers,
    /// using [delta-encoding](https://en.wikipedia.org/wiki/Delta_encoding)
    /// and variable bytes encoding.
    ///
    /// The method takes an array of ints to compress, and returns
    /// a `&[u8]` representing the compressed data.
    ///
    /// The method also takes an offset to give the value of the
    /// hypothetical previous element in the delta-encoding.
    fn compress_vint_sorted(&mut self, input: &[u32], offset: u32) -> &[u8];

    /// Compresses an array of `u32` integers,
    /// using variable bytes encoding.
    ///
    /// The method takes an array of ints to compress, and returns
    /// a `&[u8]` representing the compressed data.
    fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8];
}

pub trait VIntDecoder {
    /// Uncompress an array of `u32` integers,
    /// that were compressed using [delta-encoding](https://en.wikipedia.org/wiki/Delta_encoding)
    /// and variable bytes encoding.
    ///
    /// The method takes a number of int to decompress, and returns
    /// the amount of bytes that were read to decompress them.
    ///
    /// The method also takes an offset to give the value of the
    /// hypothetical previous element in the delta-encoding.
    ///
    /// For instance, if delta encoded are `1, 3, 9`, and the
    /// `offset` is 5, then the output will be:
    /// `5 + 1 = 6, 6 + 3= 9, 9 + 9 = 18`
    fn uncompress_vint_sorted<'a>(
        &mut self,
        compressed_data: &'a [u8],
        offset: u32,
        num_els: usize,
    ) -> usize;

    /// Uncompress an array of `u32s`, compressed using variable
    /// byte encoding.
    ///
    /// The method takes a number of int to decompress, and returns
    /// the amount of bytes that were read to decompress them.
    fn uncompress_vint_unsorted<'a>(&mut self, compressed_data: &'a [u8], num_els: usize) -> usize;
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
    fn uncompress_vint_sorted<'a>(
        &mut self,
        compressed_data: &'a [u8],
        offset: u32,
        num_els: usize,
    ) -> usize {
        self.output_len = num_els;
        vint::uncompress_sorted(compressed_data, &mut self.output[..num_els], offset)
    }

    fn uncompress_vint_unsorted<'a>(&mut self, compressed_data: &'a [u8], num_els: usize) -> usize {
        self.output_len = num_els;
        vint::uncompress_unsorted(compressed_data, &mut self.output[..num_els])
    }
}

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
            let consumed_num_bytes = decoder.uncompress_block_sorted(compressed_data, 0);
            assert_eq!(consumed_num_bytes, compressed_data.len());
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
            let consumed_num_bytes = decoder.uncompress_block_sorted(compressed_data, 10);
            assert_eq!(consumed_num_bytes, compressed_data.len());
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
            let consumed_num_bytes = decoder.uncompress_block_sorted(&compressed, 10);
            assert_eq!(consumed_num_bytes, compressed.len() - 1);
            assert_eq!(compressed[consumed_num_bytes], 173u8);
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
            let consumed_num_bytes = decoder.uncompress_block_unsorted(&compressed);
            assert_eq!(consumed_num_bytes + 1, compressed.len());
            assert_eq!(compressed[consumed_num_bytes], 173u8);
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
                let consumed_num_bytes =
                    decoder.uncompress_vint_sorted(&encoded_data, *offset, input.len());
                assert_eq!(consumed_num_bytes, encoded_data.len());
                assert_eq!(input, decoder.output_array());
            }
        }
    }

    #[bench]
    fn bench_compress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(COMPRESSION_BLOCK_SIZE, 0.1);
        b.iter(|| {
            encoder.compress_block_sorted(&data, 0u32);
        });
    }

    #[bench]
    fn bench_uncompress(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(COMPRESSION_BLOCK_SIZE, 0.1);
        let compressed = encoder.compress_block_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::new();
        b.iter(|| {
            decoder.uncompress_block_sorted(compressed, 0u32);
        });
    }

    #[test]
    fn test_all_docs_compression_numbits() {
        for num_bits in 0..33 {
            let mut data = [0u32; 128];
            if num_bits > 0 {
                data[0] = 1 << (num_bits - 1);
            }
            let mut encoder = BlockEncoder::new();
            let compressed = encoder.compress_block_unsorted(&data);
            assert_eq!(compressed[0] as usize, num_bits);
            assert_eq!(compressed.len(), compressed_block_size(compressed[0]));
        }
    }

    const NUM_INTS_BENCH_VINT: usize = 10;

    #[bench]
    fn bench_compress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_INTS_BENCH_VINT, 0.001);
        b.iter(|| {
            encoder.compress_vint_sorted(&data, 0u32);
        });
    }

    #[bench]
    fn bench_uncompress_vint(b: &mut Bencher) {
        let mut encoder = BlockEncoder::new();
        let data = tests::generate_array(NUM_INTS_BENCH_VINT, 0.001);
        let compressed = encoder.compress_vint_sorted(&data, 0u32);
        let mut decoder = BlockDecoder::new();
        b.iter(|| {
            decoder.uncompress_vint_sorted(compressed, 0u32, NUM_INTS_BENCH_VINT);
        });
    }

}
