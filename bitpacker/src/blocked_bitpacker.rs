use crate::BitUnpacker;

use super::{bitpacker::BitPacker, compute_num_bits};

const BLOCK_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct BlockedBitpacker {
    // bitpacked blocks
    compressed_blocks: Vec<u8>,
    // uncompressed data, collected until blocksize
    cache: Vec<u64>,
    offset_and_bits: Vec<(u32, u8)>,
    num_elem_last_block: usize,
}

impl BlockedBitpacker {
    pub fn new() -> Self {
        Self {
            compressed_blocks: vec![],
            cache: vec![],
            offset_and_bits: vec![],
            num_elem_last_block: 0,
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        self.compressed_blocks.capacity() + self.offset_and_bits.capacity() + self.cache.capacity()
    }

    pub fn add(&mut self, val: u64) {
        self.cache.push(val);
        if self.cache.len() == BLOCK_SIZE as usize {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        if self.cache.is_empty() {
            return;
        }
        let mut bit_packer = BitPacker::new();
        let offset = self.compressed_blocks.len() as u32;
        let num_bits_block = self
            .cache
            .iter()
            .map(|val| compute_num_bits(*val))
            .max()
            .unwrap();
        for val in self.cache.iter() {
            bit_packer
                .write(*val, num_bits_block, &mut self.compressed_blocks)
                .unwrap(); // write to im can't fail
        }
        bit_packer.flush(&mut self.compressed_blocks).unwrap();
        self.offset_and_bits.push((offset, num_bits_block));

        self.cache.clear();
    }
    pub fn finish(&mut self) {
        self.num_elem_last_block = self.cache.len();
        self.flush();
        //add padding
        self.compressed_blocks
            .resize(self.compressed_blocks.len() + 8, 0);
    }
    pub fn get(&self, idx: usize) -> u64 {
        let metadata_pos = idx / BLOCK_SIZE as usize;
        let pos_in_block = idx % BLOCK_SIZE as usize;
        let (block_pos, num_bits) = self.offset_and_bits[metadata_pos];
        let unpacked = BitUnpacker::new(num_bits).get(
            pos_in_block as u64,
            &self.compressed_blocks[block_pos as usize..],
        );
        unpacked
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        let num_elems = if self.offset_and_bits.is_empty() {
            0
        } else {
            (self.offset_and_bits.len() - 1) * 128 + self.num_elem_last_block
        };
        let iter = (0..num_elems).map(move |idx| self.get(idx));
        iter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocked_bitpacker_empty() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        blocked_bitpacker.finish();
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![]);
    }
    #[test]
    fn blocked_bitpacker_one() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        blocked_bitpacker.add(50000);
        blocked_bitpacker.finish();
        assert_eq!(blocked_bitpacker.get(0), 50000);
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![50000]);
    }
    #[test]
    fn blocked_bitpacker_test() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..21500 {
            blocked_bitpacker.add(val);
        }
        blocked_bitpacker.finish();
        for val in 0..21500 {
            assert_eq!(blocked_bitpacker.get(val as usize), val);
        }
        assert_eq!(blocked_bitpacker.iter().count(), 21500);
        assert_eq!(blocked_bitpacker.iter().last().unwrap(), 21499);
    }
}
