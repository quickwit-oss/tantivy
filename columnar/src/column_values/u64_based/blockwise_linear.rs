use std::io::Write;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::{io, iter};

use common::{BinarySerializable, CountingWriter, DeserializeFrom, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::MonotonicallyMappableToU64;
use crate::column_values::u64_based::line::Line;
use crate::column_values::u64_based::stats_collector::compute_gcd;
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::column_values::{ColumnValues, VecColumn};

const BLOCK_SIZE: u32 = 512u32;

#[derive(Debug, Default)]
struct Block {
    line: Line,
    bit_unpacker: BitUnpacker,
    data_start_offset: usize,
}

impl BinarySerializable for Block {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.line.serialize(writer)?;
        self.bit_unpacker.bit_width().serialize(writer)?;
        Ok(())
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

struct GcdBlock {
    max_residual: u64,
    residual_span: u128,
    first_residual_span: u128,
    amplitude: u64,
    first_val: u64,
    last_val: u64,
    gcd: u64,
    num_rows: u32,
}

impl GcdBlock {
    fn num_bits(&self, column_gcd: u64) -> u64 {
        let scale = self.gcd / column_gcd;
        if scale == 1 {
            return compute_num_bits(self.max_residual) as u64;
        }
        let endpoint_delta = self.last_val.abs_diff(self.first_val).saturating_mul(scale);
        let (span, first_span) = if endpoint_delta >= 1 << 31 {
            (
                self.amplitude.saturating_mul(scale),
                self.first_val.saturating_mul(scale),
            )
        } else {
            let idx_last_val = u64::from(self.idx_last_val());
            let rounding = u64::from(
                self.last_val < self.first_val
                    || endpoint_delta / idx_last_val * idx_last_val != endpoint_delta,
            );
            (
                self.rescale(self.residual_span, scale, rounding),
                self.rescale(self.first_residual_span, scale, rounding),
            )
        };
        if first_span > u32::MAX as u64 {
            return 64;
        }
        compute_num_bits(span) as u64
    }

    fn rescale(&self, span: u128, scale: u64, rounding: u64) -> u64 {
        let Some(scaled) = span.checked_mul(u128::from(scale)) else {
            return u64::MAX;
        };
        let span = scaled.div_ceil(u128::from(self.idx_last_val())) + u128::from(rounding);
        u64::try_from(span).unwrap_or(u64::MAX)
    }

    fn idx_last_val(&self) -> u32 {
        self.num_rows.max(2) - 1
    }
}

fn exact_line_spans(block: &[u64]) -> (u128, u128) {
    let idx_last_val = (block.len() - 1) as i128;
    let slope = block[block.len() - 1] as i128 - block[0] as i128;
    let first_residual = block[0] as i128 * idx_last_val;
    let mut min_residual = i128::MAX;
    let mut max_residual = i128::MIN;
    for (x, &val) in block.iter().enumerate() {
        let residual = val as i128 * idx_last_val - x as i128 * slope;
        min_residual = min_residual.min(residual);
        max_residual = max_residual.max(residual);
    }
    (
        (max_residual - min_residual) as u128,
        (first_residual - min_residual) as u128,
    )
}

pub struct BlockwiseLinearEstimator {
    block: Vec<u64>,
    values_num_bits: u64,
    gcd_blocks: Vec<GcdBlock>,
    meta_num_bytes: u64,
}

impl Default for BlockwiseLinearEstimator {
    fn default() -> Self {
        Self {
            block: Vec::with_capacity(BLOCK_SIZE as usize),
            values_num_bits: 0u64,
            gcd_blocks: Vec::new(),
            meta_num_bytes: 0u64,
        }
    }
}

impl BlockwiseLinearEstimator {
    fn block_min_max_and_gcd(&self) -> (u64, u64, NonZeroU64) {
        let Some((&first_val, rest)) = self.block.split_first() else {
            return (0u64, 0u64, NonZeroU64::MIN);
        };
        let mut block_min = first_val;
        let mut block_max = first_val;
        let mut block_gcd: Option<NonZeroU64> = None;
        for &buffer_val in rest {
            block_min = block_min.min(buffer_val);
            block_max = block_max.max(buffer_val);
            if block_gcd.map(NonZeroU64::get) == Some(1) {
                continue;
            }
            let Some(non_zero_diff) = NonZeroU64::new(buffer_val.abs_diff(first_val)) else {
                continue;
            };
            block_gcd = Some(match block_gcd {
                Some(gcd) => compute_gcd(non_zero_diff, gcd),
                None => non_zero_diff,
            });
        }
        (block_min, block_max, block_gcd.unwrap_or(NonZeroU64::MIN))
    }

    fn flush_block_estimate(&mut self) {
        if self.block.is_empty() {
            return;
        }
        let (block_min, block_max, block_gcd) = self.block_min_max_and_gcd();
        if block_gcd.get() > 1 {
            let divider = DividerU64::divide_by(block_gcd.get());
            for buffer_val in self.block.iter_mut() {
                *buffer_val = divider.divide(*buffer_val - block_min);
            }
        } else {
            for buffer_val in self.block.iter_mut() {
                *buffer_val -= block_min;
            }
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
        let num_rows = self.block.len() as u32;
        if block_gcd.get() > 1 {
            let (residual_span, first_residual_span) = exact_line_spans(&self.block);
            self.gcd_blocks.push(GcdBlock {
                max_residual: max_value,
                residual_span,
                first_residual_span,
                amplitude: (block_max - block_min) / block_gcd.get(),
                first_val: self.block[0],
                last_val: self.block[self.block.len() - 1],
                gcd: block_gcd.get(),
                num_rows,
            });
        } else {
            self.values_num_bits += compute_num_bits(max_value) as u64 * u64::from(num_rows);
        }
        self.meta_num_bytes += 1 + line.num_bytes();
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
        let gcd = stats.gcd.get();
        let values_num_bits: u64 = self.values_num_bits
            + self
                .gcd_blocks
                .iter()
                .map(|block| block.num_bits(gcd) * u64::from(block.num_rows))
                .sum::<u64>();
        Some(4 + stats.num_bytes() + self.meta_num_bytes + values_num_bits.div_ceil(8))
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
                data_start_offset: 0,
            });
        }

        bit_packer.close(wrt)?;

        assert_eq!(blocks.len(), num_blocks);

        let mut counting_wrt = CountingWriter::wrap(wrt);
        for block in &blocks {
            block.serialize(&mut counting_wrt)?;
        }
        let footer_len = counting_wrt.written_bytes();
        (footer_len as u32).serialize(&mut counting_wrt)?;

        Ok(())
    }
}

pub struct BlockwiseLinearCodec;

impl ColumnCodec<u64> for BlockwiseLinearCodec {
    type ColumnValues = BlockwiseLinearReader;

    type Estimator = BlockwiseLinearEstimator;

    fn load(mut bytes: OwnedBytes) -> io::Result<Self::ColumnValues> {
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
            blocks: blocks.into_boxed_slice().into(),
            data,
            stats,
        })
    }
}

#[derive(Clone)]
pub struct BlockwiseLinearReader {
    blocks: Arc<[Block]>,
    data: OwnedBytes,
    stats: ColumnStats,
}

impl ColumnValues for BlockwiseLinearReader {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> u64 {
        let block_id = (idx / BLOCK_SIZE) as usize;
        let idx_within_block = idx % BLOCK_SIZE;
        let block = &self.blocks[block_id];
        let interpoled_val: u64 = block.line.eval(idx_within_block);
        let block_bytes = &self.data[block.data_start_offset..];
        let bitpacked_diff = block.bit_unpacker.get(idx_within_block, block_bytes);
        // TODO optimize me! the line parameters could be tweaked to include the multiplication and
        // remove the dependency.
        self.stats.min_value
            + self
                .stats
                .gcd
                .get()
                .wrapping_mul(interpoled_val.wrapping_add(bitpacked_diff))
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
        assert_eq!(actual_compression_rate, 0.175);
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
