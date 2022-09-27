use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::Column;

pub struct SparseCodecRoaringBitmap {
    null: RoaringBitmap,
    column: Arc<dyn Column<u64>>, // column: C,
}

impl SparseCodecRoaringBitmap {
    pub fn with_full(column: Arc<dyn Column<u64>>) -> Self {
        let mut rb = RoaringBitmap::new();
        rb.insert_range(0..column.num_vals() as u32 + 1);
        Self { null: rb, column }
    }
}

impl Column for SparseCodecRoaringBitmap {
    fn get_val(&self, idx: u64) -> u64 {
        let position_of_val = self.null.rank(idx as u32);
        self.column.get_val(position_of_val) // TODO this does not handle null!
                                             // self.null.select(num_vals)
    }

    fn min_value(&self) -> u64 {
        todo!()
    }

    fn max_value(&self) -> u64 {
        todo!()
    }

    fn num_vals(&self) -> u64 {
        todo!()
    }
}

pub struct DenseCodec {
    // the bitmap blocks of length 64 bit each
    blocks: Vec<u64>,
    // the offset for each block
    offsets: Vec<u32>,
    column: Arc<dyn Column<u64>>, // column: C,
}

impl DenseCodec {
    pub fn with_full(column: Arc<dyn Column<u64>>) -> Self {
        let num_blocks = (column.num_vals() as usize / 64) + 1;
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut offsets = Vec::with_capacity(num_blocks);
        // fill all blocks
        let mut offset = 0;
        for _block_num in 0..num_blocks {
            let block = u64::MAX;
            blocks.push(block);
            offsets.push(offset);
            offset += block.count_ones();
        }

        Self {
            blocks,
            offsets,
            column,
        }
    }
}

fn gen_mask(msb: u64) -> u64 {
    let src = 1 << msb;
    src - 1
}

fn get_bit_at(input: u64, n: u64) -> bool {
    input & (1 << n) != 0
}

impl Column for DenseCodec {
    fn get_val(&self, idx: u64) -> u64 {
        let block_pos = idx / 64;
        let pos_in_block = idx % 64;
        let offset = self.offsets[block_pos as usize];
        let bitvec = self.blocks[block_pos as usize];
        let offset_in_block = (bitvec & gen_mask(pos_in_block)).count_ones();
        let dense_idx = offset as u64 + offset_in_block as u64;
        if get_bit_at(bitvec, pos_in_block) {
            self.column.get_val(dense_idx)
        } else {
            0 // TODO null
        }
    }

    fn min_value(&self) -> u64 {
        todo!()
    }

    fn max_value(&self) -> u64 {
        todo!()
    }

    fn num_vals(&self) -> u64 {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::serialize_and_load;

    #[test]
    fn dense_test() {
        let data = (0..100u64).collect_vec();
        {
            let column = serialize_and_load(&data);
            let dense = DenseCodec::with_full(column);
            for i in 0..100 {
                dense.get_val(i);
            }
        }
    }
}
