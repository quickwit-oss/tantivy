
use super::NUM_DOCS_PER_BLOCK;

const COMPRESSED_BLOCK_MAX_SIZE: usize = NUM_DOCS_PER_BLOCK * 4 + 1; 


#[cfg(feature="simdcompression")]
mod compression {
    use libc::size_t;
    extern {
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

    pub fn compress_sorted(vals: &[u32], output: &mut [u8], offset: u32) -> usize {
        unsafe { compress_sorted_cpp(vals.as_ptr(), output.as_mut_ptr(), offset) }
    }

    pub fn uncompress_sorted(compressed_data: &[u8], output: &mut [u32], offset: u32) -> usize {
        unsafe { uncompress_sorted_cpp(compressed_data.as_ptr(), output.as_mut_ptr(), offset) }
    }

    pub fn compress_unsorted(vals: &[u32], output: &mut [u8]) -> usize {
        unsafe { compress_unsorted_cpp(vals.as_ptr(), output.as_mut_ptr()) }
    }

    pub fn uncompress_unsorted(compressed_data: &[u8], output: &mut [u32]) -> usize {
        unsafe { uncompress_unsorted_cpp(compressed_data.as_ptr(), output.as_mut_ptr()) }
    }
}


#[cfg(not(feature="simdcompression"))]
mod compression {
    
    use compression::NUM_DOCS_PER_BLOCK;
    use common::bitpacker::compute_num_bits;
    use common::bitpacker::{BitPacker, BitUnpacker};
    use std::cmp;
    use std::io::Write;
    
    pub fn compress_sorted(vals: &[u32], mut output: &mut [u8], offset: u32) -> usize {
        // TODO remove the alloc
        let mut deltas = Vec::with_capacity(NUM_DOCS_PER_BLOCK);
        unsafe { deltas.set_len(NUM_DOCS_PER_BLOCK  ); }
        let mut max_delta = 0; 
        {
            let mut local_offset = offset;
            for i in 0..NUM_DOCS_PER_BLOCK {
                let val = vals[i];
                let delta = val - local_offset;
                max_delta = cmp::max(max_delta, delta);
                deltas[i] = delta;
                local_offset = val;
            }
        }
        let num_bits = compute_num_bits(max_delta);
        output.write_all(&[num_bits]).unwrap();
        let mut bit_packer = BitPacker::new(num_bits as usize);
        for val in &deltas {
            bit_packer.write(*val, &mut output).unwrap();
        }
        1 + bit_packer.close(&mut output).expect("packing in memory should never fail")
    }

    pub fn uncompress_sorted(compressed_data: &[u8], output: &mut [u32], mut offset: u32) -> usize {
        let num_bits = compressed_data[0];
        let bit_unpacker = BitUnpacker::new(&compressed_data[1..], num_bits as usize);
        for i in 0..NUM_DOCS_PER_BLOCK {
            let delta = bit_unpacker.get(i);
            let val = offset + delta;
            output[i] = val;
            offset = val;
        }
        1 + (num_bits as usize * NUM_DOCS_PER_BLOCK + 7) / 8
    }

    pub fn compress_unsorted(vals: &[u32], mut output: &mut [u8]) -> usize {
        let max = vals.iter().cloned().max().expect("compress unsorted called with an empty array");
        let num_bits = compute_num_bits(max);
        output.write_all(&[num_bits]).unwrap();
        let mut bit_packer = BitPacker::new(num_bits as usize);
        for val in vals {
            bit_packer.write(*val, &mut output).unwrap();
        }
        1 + bit_packer.close(&mut output).expect("packing in memory should never fail")
    }

    pub fn uncompress_unsorted(compressed_data: &[u8], output: &mut [u32]) -> usize {
        let num_bits = compressed_data[0];
        let bit_unpacker = BitUnpacker::new(&compressed_data[1..], num_bits as usize);
        for i in 0..NUM_DOCS_PER_BLOCK {
            output[i] = bit_unpacker.get(i);
        }
        1 + (num_bits as usize * NUM_DOCS_PER_BLOCK + 7) / 8
    }
}


pub struct BlockEncoder {
    output: [u8; COMPRESSED_BLOCK_MAX_SIZE],
    output_len: usize,
}

impl BlockEncoder {
    
    pub fn new() -> BlockEncoder {
        BlockEncoder {
            output: [0u8; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
        }    
    }
    
    pub fn compress_block_sorted(&mut self, vals: &[u32], offset: u32) -> &[u8] {
        let compressed_size = compression::compress_sorted(vals, &mut self.output, offset);
        &self.output[..compressed_size]
    }
    
    pub fn compress_block_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        let compressed_size = compression::compress_unsorted(vals, &mut self.output);
        &self.output[..compressed_size]
    }
    
    pub fn compress_vint_sorted(&mut self, input: &[u32], mut offset: u32) -> &[u8] {
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
    
    pub fn compress_vint_unsorted(&mut self, input: &[u32]) -> &[u8] {
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

pub struct BlockDecoder {
    output: [u32; COMPRESSED_BLOCK_MAX_SIZE],
    output_len: usize,
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
    
    pub fn uncompress_block_sorted<'a>(&mut self, compressed_data: &'a [u8], offset: u32) -> &'a[u8] {
        let consumed_size = compression::uncompress_sorted(compressed_data, &mut self.output, offset);
        self.output_len = NUM_DOCS_PER_BLOCK;
        &compressed_data[consumed_size..]
    }
    
    pub fn uncompress_block_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> &'a[u8] {
        let consumed_size = compression::uncompress_unsorted(compressed_data, &mut self.output);
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
    
    #[inline]
    pub fn output_array(&self,) -> &[u32] {
        &self.output[..self.output_len]
    }
    
    #[inline]
    pub fn output(&self, idx: usize) -> u32 {
        self.output[idx]
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



    