use std::io::Write;
use std::sync::Arc;
use std::{io, iter};

use common::{BinarySerializable, DeserializeFrom, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::MonotonicallyMappableToU64;
use crate::column_values::u64_based::line::Line;
use crate::column_values::u64_based::{ColumnCodecEstimator, ColumnStats};
use crate::column_values::{ColumnValues, VecColumn};

mod v2;
use v2::BlockWidths;
pub use v2::BlockwiseLinearCodec;

const BLOCK_SIZE: u32 = 512u32;

#[derive(Clone, Copy, Debug, Default)]
struct Block {
    line: Line,
    bit_unpacker: BitUnpacker,
    data_start_offset: usize,
}

impl Block {
    #[inline]
    fn get_val(&self, idx: u32, data: &[u8], stats: &ColumnStats) -> u64 {
        let diff = self.bit_unpacker.get(idx, &data[self.data_start_offset..]);
        stats.min_value
            + stats
                .gcd
                .get()
                .wrapping_mul(self.line.eval(idx).wrapping_add(diff))
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let line = Line::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        Ok(Block {
            line,
            bit_unpacker: BitUnpacker::new(bit_width),
            data_start_offset: 0,
        })
    }
}

fn compute_num_blocks(num_vals: u32) -> u32 {
    num_vals.div_ceil(BLOCK_SIZE)
}

pub struct BlockwiseLinearEstimator {
    block: Vec<u64>,
    values_num_bytes: u64,
    widths: BlockWidths,
}

impl Default for BlockwiseLinearEstimator {
    fn default() -> Self {
        Self {
            block: Vec::with_capacity(BLOCK_SIZE as usize),
            values_num_bytes: 0u64,
            widths: BlockWidths::default(),
        }
    }
}

impl BlockwiseLinearEstimator {
    fn flush_block_estimate(&mut self) {
        if self.block.is_empty() {
            return;
        }
        let column = VecColumn::from(std::mem::take(&mut self.block));
        let line = Line::train(&column);
        self.block = column.into();

        let mut max_value = 0u64;
        for (i, buffer_val) in self.block.iter().enumerate() {
            let interpolated_val = line.eval(i as u32);
            let val = buffer_val.wrapping_sub(interpolated_val);
            max_value = val.max(max_value);
        }
        let bit_width = compute_num_bits(max_value) as usize;
        self.widths.collect(line, self.values_num_bytes);
        self.values_num_bytes += (bit_width * self.block.len() + 7) as u64 / 8;
    }
}

impl ColumnCodecEstimator for BlockwiseLinearEstimator {
    fn collect(&mut self, value: u64) {
        self.block.push(value);
        if self.block.len() == BLOCK_SIZE as usize {
            self.flush_block_estimate();
            self.block.clear();
        }
    }
    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let metadata_size =
            3 + self.widths.record_size() as u64 * compute_num_blocks(stats.num_rows) as u64;
        let mut estimate = stats.num_bytes() + metadata_size + self.values_num_bytes;
        if stats.gcd.get() > 1 {
            let estimate_gain_from_gcd =
                (stats.gcd.get() as f32).log2().floor() * stats.num_rows as f32 / 8.0f32;
            estimate = estimate.saturating_sub(estimate_gain_from_gcd as u64);
        }
        Some(estimate)
    }

    fn finalize(&mut self) {
        self.flush_block_estimate();
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        mut vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let mut buffer = Vec::with_capacity(BLOCK_SIZE as usize);
        let num_blocks = compute_num_blocks(stats.num_rows) as usize;
        let mut blocks = Vec::with_capacity(num_blocks);

        let mut bit_packer = BitPacker::new();
        let mut data_start_offset = 0;

        let gcd_divider = DividerU64::divide_by(stats.gcd.get());

        for _ in 0..num_blocks {
            buffer.clear();
            buffer.extend(
                (&mut vals)
                    .map(MonotonicallyMappableToU64::to_u64)
                    .take(BLOCK_SIZE as usize),
            );

            for buffer_val in buffer.iter_mut() {
                *buffer_val = gcd_divider.divide(*buffer_val - stats.min_value);
            }

            let line = Line::train(&VecColumn::from(buffer.to_vec()));

            assert!(!buffer.is_empty());

            for (i, buffer_val) in buffer.iter_mut().enumerate() {
                let interpolated_val = line.eval(i as u32);
                *buffer_val = buffer_val.wrapping_sub(interpolated_val);
            }

            let bit_width = buffer.iter().copied().map(compute_num_bits).max().unwrap();

            for &buffer_val in &buffer {
                bit_packer.write(buffer_val, bit_width, wrt)?;
            }

            blocks.push(Block {
                line,
                bit_unpacker: BitUnpacker::new(bit_width),
                data_start_offset,
            });
            data_start_offset += (bit_width as usize * buffer.len()).div_ceil(8);
        }

        bit_packer.close(wrt)?;

        assert_eq!(blocks.len(), num_blocks);

        v2::serialize_blocks(&blocks, wrt)
    }
}

impl BlockwiseLinearCodec {
    pub fn load_v1(mut bytes: OwnedBytes) -> io::Result<BlockwiseLinearReader<DecodedBlocks>> {
        let stats = ColumnStats::deserialize(&mut bytes)?;
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;
        let (data, mut footer) = bytes.split(footer_offset);
        let num_blocks = compute_num_blocks(stats.num_rows);
        let mut blocks: Vec<Block> = iter::repeat_with(|| Block::deserialize(&mut footer))
            .take(num_blocks as usize)
            .collect::<io::Result<_>>()?;
        let mut start_offset = 0;
        for block in &mut blocks {
            block.data_start_offset = start_offset;
            start_offset += (block.bit_unpacker.bit_width() as usize) * BLOCK_SIZE as usize / 8;
        }
        Ok(BlockwiseLinearReader {
            blocks: DecodedBlocks(blocks.into_boxed_slice().into()),
            data,
            stats,
        })
    }
}

// Access one block's metadata; value decoding is shared across storage layouts.
trait BlockMetadata: Send + Sync + 'static {
    fn get_block(&self, block_id: usize) -> Block;
}

/// Decoded block metadata for the legacy format.
#[derive(Clone)]
pub struct DecodedBlocks(Arc<[Block]>);

impl BlockMetadata for DecodedBlocks {
    #[inline]
    fn get_block(&self, block_id: usize) -> Block {
        self.0[block_id]
    }
}

#[derive(Clone)]
pub struct BlockwiseLinearReader<M> {
    blocks: M,
    data: OwnedBytes,
    stats: ColumnStats,
}

impl<M: BlockMetadata> ColumnValues for BlockwiseLinearReader<M> {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> u64 {
        let block_id = (idx / BLOCK_SIZE) as usize;
        let idx_within_block = idx % BLOCK_SIZE;
        self.blocks
            .get_block(block_id)
            .get_val(idx_within_block, &self.data, &self.stats)
    }

    fn get_range(&self, start: u64, mut output: &mut [u64]) {
        let mut start = start as u32;
        while !output.is_empty() {
            let block = self.blocks.get_block((start / BLOCK_SIZE) as usize);
            let within_block = start % BLOCK_SIZE;
            let len = output.len().min((BLOCK_SIZE - within_block) as usize);
            let (head, tail) = output.split_at_mut(len);
            block
                .bit_unpacker
                .get_range(within_block, &self.data[block.data_start_offset..], head);
            for (i, value) in head.iter_mut().enumerate() {
                *value = self.stats.min_value
                    + self.stats.gcd.get().wrapping_mul(
                        block
                            .line
                            .eval(within_block + i as u32)
                            .wrapping_add(*value),
                    );
            }
            start += len as u32;
            output = tail;
        }
    }

    #[inline(always)]
    fn min_value(&self) -> u64 {
        self.stats.min_value
    }

    #[inline(always)]
    fn max_value(&self) -> u64 {
        self.stats.max_value
    }

    #[inline(always)]
    fn num_vals(&self) -> u32 {
        self.stats.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;

    // A block boundary where a high run ends and a low run begins: y0 ≈ 2^32, y511 ≈ 0.
    // This large jump used to cause an overflow which made us render all value on 64b
    // when 32 was enough.
    fn large_descending_jump_vals() -> Vec<u64> {
        let high_start: u64 = 4_294_967_039; // ≈ 2^32 - 257
        (0u64..256)
            .map(|i| high_start + i)
            .chain(0u64..256)
            .collect()
    }

    #[test]
    fn test_blockwise_linear_large_descending_jump_uses_at_most_32bit() {
        let vals = large_descending_jump_vals();
        let (_, actual_rate) =
            create_and_validate::<BlockwiseLinearCodec>(&vals, "large descending jump").unwrap();
        assert!(
            actual_rate <= 0.6,
            "compression rate {actual_rate:.3} is too high (bug: 64-bit residuals)"
        );
    }

    #[test]
    fn test_with_codec_data_sets_simple() {
        create_and_validate::<BlockwiseLinearCodec>(
            &[11, 20, 40, 20, 10, 10, 10, 10, 10, 10],
            "simple test",
        )
        .unwrap();
    }

    #[test]
    fn test_with_codec_data_sets_simple_gcd() {
        let (_, actual_compression_rate) = create_and_validate::<BlockwiseLinearCodec>(
            &[10, 20, 40, 20, 10, 10, 10, 10, 10, 10],
            "name",
        )
        .unwrap();
        assert_eq!(actual_compression_rate, 0.1375);
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = crate::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BlockwiseLinearCodec>(&data, name);
            data.reverse();
            create_and_validate::<BlockwiseLinearCodec>(&data, name);
        }
    }

    #[test]
    fn test_blockwise_linear_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BlockwiseLinearCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<BlockwiseLinearCodec>(&data, "rand");
        }
    }
}
