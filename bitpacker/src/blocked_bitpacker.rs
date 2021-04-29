use super::{bitpacker::BitPacker, compute_num_bits};

#[derive(Debug, Clone)]
struct BlockedBitpacker {
    compressed_blocks: Vec<u8>,
    cache: Vec<u64>,
    offset_and_bits: Vec<(u32, u8)>,
    blocksize: u32,
}

impl BlockedBitpacker {
    fn new(blocksize: u32) -> Self {
        Self {
            compressed_blocks: vec![],
            cache: vec![],
            offset_and_bits: vec![],
            blocksize,
        }
    }

    fn add(&mut self, el: u64) {
        self.cache.push(el);
        if self.cache.len() > self.blocksize as usize {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.cache.is_empty() {
            return;
        }
        let mut bit_packer = BitPacker::new();
        let num_bits_block = self
            .cache
            .iter()
            .map(|el| compute_num_bits(*el))
            .max()
            .unwrap();
        for val in self.cache.iter() {
            bit_packer
                .write(*val, num_bits_block, &mut self.compressed_blocks)
                .unwrap(); // write to im can't fail
        }
        self.offset_and_bits
            .push((self.compressed_blocks.len() as u32, num_bits_block));
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    test
}
