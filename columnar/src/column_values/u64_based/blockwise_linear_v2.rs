//! BlockwiseLinearV2 — a u64 column codec optimised for lazy, on-demand access.
//!
//! Like BlockwiseLinear (V1), values are split into fixed-size blocks of 512 rows.
//! Each block fits a linear approximation and bitpacks the residuals. The key
//! difference is in the footer layout and load strategy:
//!
//! **Fixed-size footer entries (21 bytes each):**
//!   `slope(8) + intercept(8) + bit_width(1) + data_offset(4)`
//!
//! The fixed entry size enables O(1) block lookup by index, which in turn allows:
//!
//! - **Lazy `load()`** — the footer is kept as a `FileSlice` and never eagerly read. Block metadata
//!   is parsed on demand (21 bytes) only for accessed blocks, giving near-zero load cost for TopK
//!   queries that touch few rows.
//!
//! - **Block-aware `get_row_ids_for_value_range`** — reads the footer range for the needed blocks
//!   in one `read_bytes()` call, then one `read_bytes()` per block. Replaces the default per-row
//!   `get_val` loop for filtered range scans.
//!
//! - **Single-entry block cache in `get_val`** — sequential doc-ID access hits the cache ~512 times
//!   per block, reducing `read_bytes` calls from O(N) to O(N/512).
//!
//! The footer is roughly 2–4× larger per block than V1 (~42 KB for 1M rows vs
//! ~12–18 KB), but this is negligible relative to the data region.

use std::io;
use std::io::Write;
use std::ops::{Range, RangeInclusive};

use common::file_slice::FileSlice;
use common::{BinarySerializable, CountingWriter, DeserializeFrom, FixedSize, HasLen, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::MonotonicallyMappableToU64;
use crate::column_values::u64_based::line::Line;
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::column_values::{ColumnValues, VecColumn};

const BLOCK_SIZE: u32 = 512u32;
/// Bytes per block when bit_width=1: one bit per value × BLOCK_SIZE values / 8 bits-per-byte.
/// Each additional bit of `bit_width` adds another `BYTES_PER_BLOCK_BIT` bytes.
/// `data_offset` is stored in these units so a u32 can address up to ~256GB.
const BYTES_PER_BLOCK_BIT: usize = (BLOCK_SIZE / 8) as usize; // 64

/// Fixed-size footer entry: slope(8) + intercept(8) + bit_width(1) + data_offset(4) = 21 bytes.
/// `data_offset` is stored in units of 64 bytes (BLOCK_SIZE / 8), not raw bytes,
/// extending the addressable range from ~4GB to ~256GB per column per segment.
#[derive(Debug, Clone, Copy)]
struct BlockMeta {
    line: Line,
    bit_width: u8,
    /// Offset into the data region in units of 64 bytes.
    data_offset: u32,
}

const BLOCK_META_SIZE: usize = <u64 as FixedSize>::SIZE_IN_BYTES  // slope
    + <u64 as FixedSize>::SIZE_IN_BYTES  // intercept
    + <u8 as FixedSize>::SIZE_IN_BYTES   // bit_width
    + <u32 as FixedSize>::SIZE_IN_BYTES; // data_offset

impl BlockMeta {
    /// Returns the byte range of this block's bitpacked data within the data region.
    #[inline(always)]
    fn data_byte_range(&self, data_region_len: usize) -> std::ops::Range<usize> {
        let start = self.data_offset as usize * BYTES_PER_BLOCK_BIT;
        let end = (start + self.bit_width as usize * BYTES_PER_BLOCK_BIT).min(data_region_len);
        start..end
    }
}

impl BinarySerializable for BlockMeta {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        // We serialize slope/intercept individually instead of `line.serialize()`
        // to ensure fixed-sized encoding (the latter uses varints).
        // We do manual serialization instead of `line.serialize()`
        // to ensure everything is fixed-sized (the latter uses varints).
        self.line.slope.serialize(writer)?;
        self.line.intercept.serialize(writer)?;
        self.bit_width.serialize(writer)?;
        self.data_offset.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let slope = u64::deserialize(reader)?;
        let intercept = u64::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        let data_offset = u32::deserialize(reader)?;
        Ok(BlockMeta {
            line: Line { slope, intercept },
            bit_width,
            data_offset,
        })
    }
}

impl FixedSize for BlockMeta {
    const SIZE_IN_BYTES: usize = BLOCK_META_SIZE;
}

#[inline(always)]
fn parse_block_meta(mut entry: &[u8]) -> BlockMeta {
    BlockMeta::deserialize(&mut entry).expect("failed to parse block meta")
}

fn compute_num_blocks(num_vals: u32) -> u32 {
    num_vals.div_ceil(BLOCK_SIZE)
}

pub(crate) struct BlockwiseLinearV2Estimator {
    block: Vec<u64>,
    values_num_bytes: u64,
    num_blocks: u64,
}

impl Default for BlockwiseLinearV2Estimator {
    fn default() -> Self {
        Self {
            block: Vec::with_capacity(BLOCK_SIZE as usize),
            values_num_bytes: 0u64,
            num_blocks: 0u64,
        }
    }
}

impl BlockwiseLinearV2Estimator {
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
        self.values_num_bytes += (bit_width * self.block.len()).div_ceil(8) as u64;
        self.num_blocks += 1;
    }
}

impl ColumnCodecEstimator for BlockwiseLinearV2Estimator {
    fn collect(&mut self, value: u64) {
        self.block.push(value);
        if self.block.len() == BLOCK_SIZE as usize {
            self.flush_block_estimate();
            self.block.clear();
        }
    }

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let meta_num_bytes = self.num_blocks * BLOCK_META_SIZE as u64;
        let mut estimate = 4 + stats.num_bytes() + meta_num_bytes + self.values_num_bytes;
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
        let mut blocks: Vec<(Line, u8)> = Vec::with_capacity(num_blocks);

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

            blocks.push((line, bit_width));
        }

        bit_packer.close(wrt)?;

        assert_eq!(blocks.len(), num_blocks);

        // Write fixed-size footer.
        let mut counting_wrt = CountingWriter::wrap(wrt);
        let mut data_offset = 0u32;
        for &(line, bit_width) in &blocks {
            BlockMeta {
                line,
                bit_width,
                data_offset,
            }
            .serialize(&mut counting_wrt)?;
            data_offset += bit_width as u32;
        }
        let footer_len = counting_wrt.written_bytes();
        (footer_len as u32).serialize(&mut counting_wrt)?;

        Ok(())
    }
}

pub(crate) struct BlockwiseLinearV2Codec;

impl ColumnCodec<u64> for BlockwiseLinearV2Codec {
    type ColumnValues = BlockwiseLinearV2Reader;
    type Estimator = BlockwiseLinearV2Estimator;

    fn load(file_slice: FileSlice) -> io::Result<Self::ColumnValues> {
        let (stats, body) = ColumnStats::deserialize_from_tail(file_slice)?;

        let (_, footer_len_slice) = body.clone().split_from_end(4);
        let footer_len: u32 = footer_len_slice.read_bytes()?.as_slice().deserialize()?;
        let (data, footer) = body.split_from_end(footer_len as usize + 4);
        // footer is kept as FileSlice — NOT eagerly read into OwnedBytes.
        // Individual block metadata is read on demand (21 bytes per block).

        Ok(BlockwiseLinearV2Reader {
            footer,
            data,
            stats,
            cache: BlockCache::new(),
        })
    }
}

struct CachedBlock {
    block_id: u32,
    line: Line,
    bit_unpacker: BitUnpacker,
    data: OwnedBytes,
}

struct BlockCache(std::cell::RefCell<CachedBlock>);

// Safety: ColumnValues requires Send + Sync, but in practice readers are not
// shared across threads concurrently — each query thread gets its own clone.
unsafe impl Send for BlockCache {}
unsafe impl Sync for BlockCache {}

impl Clone for BlockCache {
    fn clone(&self) -> Self {
        BlockCache::new()
    }
}

impl BlockCache {
    fn new() -> Self {
        BlockCache(std::cell::RefCell::new(CachedBlock {
            block_id: u32::MAX,
            line: Line {
                slope: 0,
                intercept: 0,
            },
            bit_unpacker: BitUnpacker::new(0),
            data: OwnedBytes::empty(),
        }))
    }
}

#[derive(Clone)]
pub(crate) struct BlockwiseLinearV2Reader {
    footer: FileSlice,
    data: FileSlice,
    stats: ColumnStats,
    cache: BlockCache,
}

impl BlockwiseLinearV2Reader {
    #[inline(always)]
    fn block_meta(&self, block_id: usize) -> BlockMeta {
        let off = block_id * BLOCK_META_SIZE;
        let entry = self
            .footer
            .slice(off..off + BLOCK_META_SIZE)
            .read_bytes()
            .expect("failed to read block meta");
        parse_block_meta(&entry)
    }
}

impl ColumnValues for BlockwiseLinearV2Reader {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> u64 {
        let block_id = idx / BLOCK_SIZE;
        let idx_within_block = idx % BLOCK_SIZE;
        let mut cache = self.cache.0.borrow_mut();
        if cache.block_id != block_id {
            let meta = self.block_meta(block_id as usize);
            let range = meta.data_byte_range(self.data.len());
            cache.block_id = block_id;
            cache.line = meta.line;
            cache.bit_unpacker = BitUnpacker::new(meta.bit_width);
            cache.data = self
                .data
                .slice(range)
                .read_bytes()
                .expect("failed to read block data");
        }
        let interpoled_val: u64 = cache.line.eval(idx_within_block);
        let bitpacked_diff = cache.bit_unpacker.get(idx_within_block, &cache.data);
        self.stats.min_value
            + self
                .stats
                .gcd
                .get()
                .wrapping_mul(interpoled_val.wrapping_add(bitpacked_diff))
    }

    fn get_row_ids_for_value_range(
        &self,
        value_range: RangeInclusive<u64>,
        row_id_range: Range<u32>,
        row_id_hits: &mut Vec<u32>,
    ) {
        if value_range.is_empty() {
            return;
        }
        let row_id_range = row_id_range.start..row_id_range.end.min(self.stats.num_rows);
        if row_id_range.is_empty() {
            return;
        }

        let min_value = self.stats.min_value;
        let gcd = self.stats.gcd.get();

        let first_block = (row_id_range.start / BLOCK_SIZE) as usize;
        let last_block = ((row_id_range.end - 1) / BLOCK_SIZE) as usize;

        // Read footer once for the block range — avoids per-block read_bytes on footer.
        let footer_start = first_block * BLOCK_META_SIZE;
        let footer_end = (last_block + 1) * BLOCK_META_SIZE;
        let footer_bytes = self
            .footer
            .slice(footer_start..footer_end.min(self.footer.len()))
            .read_bytes()
            .expect("failed to read footer range");

        for block_id in first_block..=last_block {
            // Parse block metadata from the pre-read footer bytes.
            let local_off = (block_id - first_block) * BLOCK_META_SIZE;
            let meta = parse_block_meta(&footer_bytes[local_off..local_off + BLOCK_META_SIZE]);

            let range = meta.data_byte_range(self.data.len());
            let data = self
                .data
                .slice(range)
                .read_bytes()
                .expect("failed to read block data");
            let bit_unpacker = BitUnpacker::new(meta.bit_width);

            let block_start_row = block_id as u32 * BLOCK_SIZE;
            let local_start = if block_start_row < row_id_range.start {
                row_id_range.start - block_start_row
            } else {
                0
            };
            let local_end = (row_id_range.end - block_start_row).min(BLOCK_SIZE);

            for idx_within_block in local_start..local_end {
                let interpoled_val = meta.line.eval(idx_within_block);
                let bitpacked_diff = bit_unpacker.get(idx_within_block, &data);
                let val = min_value + gcd.wrapping_mul(interpoled_val.wrapping_add(bitpacked_diff));
                if value_range.contains(&val) {
                    row_id_hits.push(block_start_row + idx_within_block);
                }
            }
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

    #[test]
    fn test_v2_simple() {
        create_and_validate::<BlockwiseLinearV2Codec>(
            &[11, 20, 40, 20, 10, 10, 10, 10, 10, 10],
            "simple test",
        )
        .unwrap();
    }

    #[test]
    fn test_v2_gcd() {
        create_and_validate::<BlockwiseLinearV2Codec>(
            &[10, 20, 40, 20, 10, 10, 10, 10, 10, 10],
            "name",
        )
        .unwrap();
    }

    #[test]
    fn test_v2_datasets() {
        let data_sets = crate::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BlockwiseLinearV2Codec>(&data, name);
            data.reverse();
            create_and_validate::<BlockwiseLinearV2Codec>(&data, name);
        }
    }

    #[test]
    fn test_v2_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BlockwiseLinearV2Codec>(&data, "rand");
            data.reverse();
            create_and_validate::<BlockwiseLinearV2Codec>(&data, "rand");
        }
    }
}
