use common::bitpacker::compute_num_bits;
use common::bitpacker::{BitPacker, BitUnpacker};
use common::CountingWriter;
use std::cmp;
use std::io::Write;
use super::super::{compute_block_size, COMPRESSION_BLOCK_SIZE};

const COMPRESSED_BLOCK_MAX_SIZE: usize = COMPRESSION_BLOCK_SIZE * 4 + 1;

pub fn compress_sorted(vals: &mut [u32], output: &mut [u8], offset: u32) -> usize {
    let mut max_delta = 0;
    {
        let mut local_offset = offset;
        for i in 0..COMPRESSION_BLOCK_SIZE {
            let val = vals[i];
            let delta = val - local_offset;
            max_delta = cmp::max(max_delta, delta);
            vals[i] = delta;
            local_offset = val;
        }
    }
    let mut counting_writer = CountingWriter::wrap(output);
    let num_bits = compute_num_bits(max_delta as u64);
    counting_writer.write_all(&[num_bits]).unwrap();

    let mut bit_packer = BitPacker::new();
    for val in vals {
        bit_packer
            .write(*val as u64, num_bits, &mut counting_writer)
            .unwrap();
    }
    let compressed_size = counting_writer.written_bytes();
    assert_eq!(compressed_size, compute_block_size(num_bits));
    compressed_size
}

pub struct BlockEncoder {
    pub output: [u8; COMPRESSED_BLOCK_MAX_SIZE],
    pub output_len: usize,
    input_buffer: [u32; COMPRESSION_BLOCK_SIZE],
}

impl BlockEncoder {
    pub fn new() -> BlockEncoder {
        BlockEncoder {
            output: [0u8; COMPRESSED_BLOCK_MAX_SIZE],
            output_len: 0,
            input_buffer: [0u32; COMPRESSION_BLOCK_SIZE],
        }
    }

    pub fn compress_block_sorted(&mut self, vals: &[u32], offset: u32) -> &[u8] {
        self.input_buffer.clone_from_slice(vals);
        let compressed_size = compress_sorted(&mut self.input_buffer, &mut self.output, offset);
        &self.output[..compressed_size]
    }

    pub fn compress_block_unsorted(&mut self, vals: &[u32]) -> &[u8] {
        let compressed_size = {
            let output: &mut [u8] = &mut self.output;
            let max = vals.iter()
                .cloned()
                .max()
                .expect("compress unsorted called with an empty array");
            let num_bits = compute_num_bits(max as u64);
            let mut counting_writer = CountingWriter::wrap(output);
            counting_writer.write_all(&[num_bits]).unwrap();
            let mut bit_packer = BitPacker::new();
            for val in vals {
                bit_packer
                    .write(*val as u64, num_bits, &mut counting_writer)
                    .unwrap();
            }
            for _ in vals.len()..COMPRESSION_BLOCK_SIZE {
                bit_packer
                    .write(vals[0] as u64, num_bits, &mut counting_writer)
                    .unwrap();
            }
            bit_packer.flush(&mut counting_writer).expect(
                "Flushing the bitpacking \
                 in an in RAM buffer should never fail",
            );
            // we avoid writing "closing", because we
            // do not want 7 bytes of padding here.
            counting_writer.written_bytes()
        };
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

    pub fn uncompress_block_sorted<'a>(
        &mut self,
        compressed_data: &'a [u8],
        mut offset: u32,
    ) -> usize {
        let consumed_size = {
            let num_bits = compressed_data[0];
            let bit_unpacker = BitUnpacker::new(&compressed_data[1..], num_bits as usize);
            for i in 0..COMPRESSION_BLOCK_SIZE {
                let delta = bit_unpacker.get(i);
                let val = offset + delta as u32;
                self.output[i] = val;
                offset = val;
            }
            compute_block_size(num_bits)
        };
        self.output_len = COMPRESSION_BLOCK_SIZE;
        consumed_size
    }

    pub fn uncompress_block_unsorted<'a>(&mut self, compressed_data: &'a [u8]) -> usize {
        let num_bits = compressed_data[0];
        let bit_unpacker = BitUnpacker::new(&compressed_data[1..], num_bits as usize);
        for i in 0..COMPRESSION_BLOCK_SIZE {
            self.output[i] = bit_unpacker.get(i) as u32;
        }
        let consumed_size = 1 + (num_bits as usize * COMPRESSION_BLOCK_SIZE + 7) / 8;
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
