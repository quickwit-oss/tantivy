use crate::BitUnpacker;

use super::{bitpacker::BitPacker, compute_num_bits};

const BLOCK_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct BlockedBitpacker {
    // bitpacked blocks
    compressed_blocks: Vec<u8>,
    // uncompressed data, collected until BLOCK_SIZE
    cache: Vec<u64>,
    offset_and_bits: Vec<(u32, u8)>,
}

impl BlockedBitpacker {
    pub fn new() -> Self {
        let mut compressed_blocks = vec![];
        compressed_blocks.resize(8, 0);
        Self {
            compressed_blocks,
            cache: vec![],
            offset_and_bits: vec![],
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
        let num_bits_block = self
            .cache
            .iter()
            .map(|val| compute_num_bits(*val))
            .max()
            .unwrap();
        self.compressed_blocks
            .resize(self.compressed_blocks.len() - 8, 0); // remove padding for bitpacker
        let offset = self.compressed_blocks.len() as u32;
        for val in self.cache.iter() {
            bit_packer
                .write(*val, num_bits_block, &mut self.compressed_blocks)
                .expect("cannot write bitpacking to output"); // write to im can't fail
        }
        bit_packer.flush(&mut self.compressed_blocks).unwrap();
        self.offset_and_bits.push((offset, num_bits_block));

        self.cache.clear();
        self.compressed_blocks
            .resize(self.compressed_blocks.len() + 8, 0); // add padding for bitpacker
    }
    pub fn get(&self, idx: usize) -> u64 {
        let metadata_pos = idx / BLOCK_SIZE as usize;
        let pos_in_block = idx % BLOCK_SIZE as usize;
        if let Some((block_pos, num_bits)) = self.offset_and_bits.get(metadata_pos).cloned() {
            //let (block_pos, num_bits) = self.offset_and_bits[metadata_pos];
            let unpacked = BitUnpacker::new(num_bits).get(
                pos_in_block as u64,
                &self.compressed_blocks[block_pos as usize..],
            );
            unpacked
        } else {
            self.cache[pos_in_block]
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        let bitpacked_elems = self.offset_and_bits.len() * BLOCK_SIZE;
        let iter = (0..bitpacked_elems)
            .map(move |idx| self.get(idx))
            .chain(self.cache.iter().cloned());
        iter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocked_bitpacker_empty() {
        let blocked_bitpacker = BlockedBitpacker::new();
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![]);
    }
    #[test]
    fn blocked_bitpacker_one() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        blocked_bitpacker.add(50000);
        assert_eq!(blocked_bitpacker.get(0), 50000);
        assert_eq!(blocked_bitpacker.iter().collect::<Vec<u64>>(), vec![50000]);
    }
    #[test]
    fn blocked_bitpacker_test() {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..21500 {
            blocked_bitpacker.add(val);
        }
        for val in 0..21500 {
            assert_eq!(blocked_bitpacker.get(val as usize), val);
        }
        assert_eq!(blocked_bitpacker.iter().count(), 21500);
        assert_eq!(blocked_bitpacker.iter().last().unwrap(), 21499);
    }
}
