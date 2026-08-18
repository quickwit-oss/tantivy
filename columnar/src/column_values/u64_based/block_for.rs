//! Per-block Frame-of-Reference codec ("BlockFor").
//!
//! Values are normalized like in `Bitpacked` (`v' = (v - column_min) / gcd`)
//! and cut into blocks of 128. Each block stores its own minimum and bit
//! width, so outliers or a drifting baseline only inflate the blocks they
//! live in.
//!
//! ## Layout
//!
//! ```text
//! [ColumnStats]
//! [block 0 packed values][block 1 packed values]...   // 16 * bit_width bytes each
//! [block records: num_blocks * stride bits]           // (offset, bit_width, block_min)
//! [16 zero bytes]
//! [offset_bit_width: u8][width_bit_width: u8][min_bit_width: u8]
//! [footer_len: u32 LE]
//! ```
//!
//! Blocks are padded to exactly 128 slots: always `16 * bit_width` bytes,
//! byte aligned, independently decodable by the shared block kernels.
//!
use std::io::{self, Write};

use common::{BinarySerializable, CountingWriter, DeserializeFrom, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, compute_num_bits};

use super::block_decode::{
    BLOCK_LEN, BlockDecode, DecodeCost, decode_block, min_batch_rows, partial_block_min_rows,
};
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::{ColumnValues, RowId};

/// Zero bytes written after the block records.
///
/// [`BlockForReader::block_meta`] reads each of a record's two halves with an
/// unaligned 8-byte load and masks; the padding is what keeps those in bounds
/// for the last blocks of every column, so neither read needs a length check
/// of its own.
const FOOTER_PAD: usize = 16;

/// Mask of the low `num_bits`, for widths up to and including 64.
#[inline(always)]
fn low_mask(num_bits: u8) -> u64 {
    if num_bits >= 64 {
        u64::MAX
    } else {
        (1u64 << num_bits) - 1
    }
}

#[derive(Clone, Copy)]
struct BlockMeta {
    start_byte_offset: u64,
    /// The block's minimum, normalized as `(v - column_min) / gcd`. Left
    /// normalized so a slot's value is `column_min + gcd * (block_min + slot)`
    /// -- one multiply per value, and a hoistable base per block.
    block_min: u64,
    bit_width: u8,
}

#[derive(Clone, Copy)]
struct RecordLayout {
    /// Bytes per record.
    stride: usize,
    offset_mask: u64,
    width_shift: u32,
    width_mask: u64,
    /// Byte offset of the minimum inside the record.
    min_byte: usize,
    min_mask: u64,
}

impl RecordLayout {
    fn new(offset_bits: u8, width_bits: u8, min_bits: u8) -> RecordLayout {
        let head_bytes = (offset_bits as usize + width_bits as usize).div_ceil(8);
        RecordLayout {
            stride: head_bytes + (min_bits as usize).div_ceil(8),
            offset_mask: low_mask(offset_bits),
            width_shift: offset_bits as u32,
            width_mask: low_mask(width_bits),
            min_byte: head_bytes,
            min_mask: low_mask(min_bits),
        }
    }
}

#[derive(Clone)]
pub struct BlockForReader {
    data: OwnedBytes,
    meta: OwnedBytes,
    layout: RecordLayout,
    num_blocks: usize,
    gcd: u64,
    stats: ColumnStats,
}

impl BlockForReader {
    #[inline(always)]
    fn block_meta(&self, block_idx: usize) -> BlockMeta {
        let record = block_idx * self.layout.stride;
        let head = u64::from_le_bytes(self.meta[record..record + 8].try_into().unwrap());
        let start_units = head & self.layout.offset_mask;
        let bit_width = (head >> self.layout.width_shift) & self.layout.width_mask;
        let min_at = record + self.layout.min_byte;
        let block_min = u64::from_le_bytes(self.meta[min_at..min_at + 8].try_into().unwrap())
            & self.layout.min_mask;
        BlockMeta {
            start_byte_offset: start_units * 16,
            block_min,
            bit_width: bit_width as u8,
        }
    }

    /// The value a `0` slot of this block maps to.
    #[inline(always)]
    fn base(&self, meta: &BlockMeta) -> u64 {
        self.stats.min_value + self.gcd * meta.block_min
    }

    /// The `bit_width`-bit slot `slot` of the block at `start_byte_offset`,
    /// still normalized. Reading 8 bytes needs no bounds handling of its own
    /// because `data` runs to the end of the column rather than stopping at
    /// the last block.
    #[inline(always)]
    fn slot(&self, meta: &BlockMeta, slot: u32) -> u64 {
        // `bit_width` is 0..=56 or 64 (`compute_num_bits`), and at 64 the bit
        // offset is a whole number of bytes, so the shift below never drops
        // bits off the top of the 8 loaded.
        let bit = slot as usize * meta.bit_width as usize;
        let byte = meta.start_byte_offset as usize + bit / 8;
        let raw = u64::from_le_bytes(self.data[byte..byte + 8].try_into().unwrap());
        (raw >> (bit % 8)) & low_mask(meta.bit_width)
    }
}

impl BlockDecode for BlockForReader {
    fn num_full_blocks(&self) -> usize {
        self.num_blocks
    }

    #[inline]
    fn partial_min_rows(&self, block_idx: usize) -> usize {
        partial_block_min_rows(self.block_meta(block_idx).bit_width)
    }

    #[inline]
    fn decode_block_mapped(&self, block_idx: usize, out: &mut [u64; BLOCK_LEN]) {
        let meta = self.block_meta(block_idx);
        decode_block(
            meta.bit_width,
            &self.data[meta.start_byte_offset as usize..],
            out,
        );
        let base = self.base(&meta);
        if self.gcd == 1 {
            for o in out.iter_mut() {
                *o += base;
            }
        } else {
            for o in out.iter_mut() {
                *o = base + self.gcd * *o;
            }
        }
    }
}

impl ColumnValues for BlockForReader {
    #[inline(always)]
    fn get_vals(self: &BlockForReader, indexes: &[u32], out: &mut [u64]) {
        let mut i: usize = 0;
        while i < indexes.len() {
            let block_idx = indexes[i] as usize / BLOCK_LEN;
            let j = i + indexes[i..].partition_point(|&idx| idx as usize / BLOCK_LEN <= block_idx);
            let meta = self.block_meta(block_idx);
            let base = self.base(&meta);
            for (k, &idx) in indexes[i..j].iter().enumerate() {
                out[i + k] = base + self.gcd * self.slot(&meta, idx % BLOCK_LEN as u32);
            }
            i = j;
        }
    }

    #[inline(always)]
    fn get_val(self: &BlockForReader, idx: u32) -> u64 {
        let meta = self.block_meta(idx as usize / BLOCK_LEN);
        let slot = self.slot(&meta, idx % BLOCK_LEN as u32);
        self.stats.min_value + self.gcd * (meta.block_min + slot)
    }

    #[inline]
    fn min_value(&self) -> u64 {
        self.stats.min_value
    }

    #[inline]
    fn max_value(&self) -> u64 {
        self.stats.max_value
    }

    #[inline]
    fn num_vals(&self) -> RowId {
        self.stats.num_rows
    }

    fn get_row_ids_for_value_range(
        &self,
        value_range: std::ops::RangeInclusive<u64>,
        row_id_range: std::ops::Range<u32>,
        row_id_hits: &mut Vec<u32>,
    ) {
        let end = row_id_range.end.min(self.num_vals());
        let start = row_id_range.start;
        if start >= end {
            return;
        }
        let (value_lo, value_hi) = (*value_range.start(), *value_range.end());
        let mut buf = [0u64; BLOCK_LEN];
        let first_block = start as usize / BLOCK_LEN;
        let last_block = (end as usize - 1) / BLOCK_LEN;
        for block_idx in first_block..=last_block {
            let meta = self.block_meta(block_idx);
            let bit_width = meta.bit_width;
            let rel_max = if bit_width >= 64 {
                u64::MAX
            } else {
                (1u64 << bit_width) - 1
            };
            let block_min = self.base(&meta);
            let block_max = block_min.saturating_add(self.gcd.saturating_mul(rel_max));
            if block_min > value_hi || block_max < value_lo {
                continue;
            }
            self.decode_block_mapped(block_idx, &mut buf);
            let row_base = (block_idx * BLOCK_LEN) as u32;
            let from = start.max(row_base) - row_base;
            let to = end.min(row_base + BLOCK_LEN as u32) - row_base;
            for offset in from..to {
                let val = buf[offset as usize];
                if value_lo <= val && val <= value_hi {
                    row_id_hits.push(row_base + offset);
                }
            }
        }
    }

    fn get_range(&self, start: u64, output: &mut [u64]) {
        assert!(
            start + output.len() as u64 <= u64::from(self.stats.num_rows),
            "get_range out of bounds"
        );
        self.decode_range(start, output);
    }

    fn min_batch_rows(&self) -> usize {
        min_batch_rows(DecodeCost::Blocked)
    }
}

pub struct BlockForEstimator {
    block: Vec<u64>,
    block_stats: Vec<(u64, u64)>,
}

impl Default for BlockForEstimator {
    fn default() -> Self {
        BlockForEstimator {
            block: Vec::with_capacity(BLOCK_LEN),
            block_stats: Vec::new(),
        }
    }
}

impl BlockForEstimator {
    fn flush_block(&mut self) {
        if self.block.is_empty() {
            return;
        }
        let min = *self.block.iter().min().unwrap();
        let max = *self.block.iter().max().unwrap();
        self.block_stats.push((min, max - min));
        self.block.clear();
    }
}

impl ColumnCodecEstimator for BlockForEstimator {
    fn collect(&mut self, value: u64) {
        self.block.push(value);
        if self.block.len() == BLOCK_LEN {
            self.flush_block();
        }
    }

    fn finalize(&mut self) {
        self.flush_block();
    }

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let gcd = stats.gcd.get();
        let num_blocks = self.block_stats.len() as u64;
        let mut total_units = 0u64;
        let mut max_width = 0u8;
        let mut max_block_min = 0u64;
        for &(block_min, amplitude) in &self.block_stats {
            let bit_width = compute_num_bits(amplitude / gcd);
            total_units += bit_width as u64;
            max_width = max_width.max(bit_width);
            max_block_min = max_block_min.max((block_min - stats.min_value) / gcd);
        }
        Some(
            stats.num_bytes()
                + 16 * total_units
                + footer_len(num_blocks, total_units, max_width, max_block_min)
                + 4,
        )
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let gcd_divider = DividerU64::divide_by(stats.gcd.get());
        let mut block: Vec<u64> = Vec::with_capacity(BLOCK_LEN);
        let mut widths: Vec<u8> = Vec::with_capacity(self.block_stats.len());
        let mut mins: Vec<u64> = Vec::with_capacity(self.block_stats.len());
        let mut bit_packer = BitPacker::new();
        loop {
            block.clear();
            block.extend((&mut *vals).take(BLOCK_LEN));
            if block.is_empty() {
                break;
            }
            for val in block.iter_mut() {
                *val = gcd_divider.divide(*val - stats.min_value);
            }
            let block_min = *block.iter().min().unwrap();
            let amplitude = *block.iter().max().unwrap() - block_min;
            let bit_width = compute_num_bits(amplitude);
            for &val in &block {
                bit_packer.write(val - block_min, bit_width, wrt)?;
            }

            for _ in block.len()..BLOCK_LEN {
                bit_packer.write(0u64, bit_width, wrt)?;
            }
            // 128 * bit_width bits is a multiple of 64: the packer is empty.
            bit_packer.flush(wrt)?;
            widths.push(bit_width);
            mins.push(block_min);
        }

        let total_units = widths.iter().map(|&w| w as u64).sum();
        let max_width = widths.iter().copied().max().unwrap_or(0);
        let max_block_min = mins.iter().copied().max().unwrap_or(0);
        let offset_bit_width = compute_num_bits(total_units);
        let width_bit_width = compute_num_bits(max_width as u64);
        let min_bit_width = compute_num_bits(max_block_min);
        let mut counting_wrt = CountingWriter::wrap(wrt);

        let head_bytes = (offset_bit_width as usize + width_bit_width as usize).div_ceil(8);
        let min_bytes = (min_bit_width as usize).div_ceil(8);
        let mut units = 0u64;
        for (&bit_width, &block_min) in widths.iter().zip(&mins) {
            let head = units | ((bit_width as u64) << offset_bit_width);
            counting_wrt.write_all(&head.to_le_bytes()[..head_bytes])?;
            counting_wrt.write_all(&block_min.to_le_bytes()[..min_bytes])?;
            units += bit_width as u64;
        }
        counting_wrt.write_all(&[0u8; FOOTER_PAD])?;
        offset_bit_width.serialize(&mut counting_wrt)?;
        width_bit_width.serialize(&mut counting_wrt)?;
        min_bit_width.serialize(&mut counting_wrt)?;
        let footer_len = u32::try_from(counting_wrt.written_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "column footer exceeds u32::MAX")
        })?;
        debug_assert_eq!(
            u64::from(footer_len),
            self::footer_len(widths.len() as u64, total_units, max_width, max_block_min)
        );
        footer_len.serialize(&mut counting_wrt)?;
        Ok(())
    }
}

fn footer_len(num_blocks: u64, total_units: u64, max_width: u8, max_block_min: u64) -> u64 {
    num_blocks
        * RecordLayout::new(
            compute_num_bits(total_units),
            compute_num_bits(max_width as u64),
            compute_num_bits(max_block_min),
        )
        .stride as u64
        + FOOTER_PAD as u64
        + 3
}

pub struct BlockForCodec;

impl ColumnCodec for BlockForCodec {
    type ColumnValues = BlockForReader;
    type Estimator = BlockForEstimator;

    fn load(mut bytes: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut bytes)?;
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;

        let data = bytes.clone();
        let meta = bytes.slice(footer_offset..bytes.len());

        let offset_bit_width = meta[meta.len() - 7];
        let width_bit_width = meta[meta.len() - 6];
        let min_bit_width = meta[meta.len() - 5];

        Ok(BlockForReader {
            data,
            meta,
            layout: RecordLayout::new(offset_bit_width, width_bit_width, min_bit_width),
            num_blocks: (stats.num_rows as usize).div_ceil(BLOCK_LEN),
            gcd: stats.gcd.get(),
            stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_block_for_simple() {
        create_and_validate::<BlockForCodec>(&[4, 3, 12], "simple");
    }

    #[test]
    fn test_block_for_gcd() {
        create_and_validate::<BlockForCodec>(&[1000, 2000, 3000], "gcd");
    }

    #[test]
    fn test_block_for_block_boundaries() {
        for num_vals in [1usize, 127, 128, 129, 255, 256, 257, 1024, 1027] {
            let vals: Vec<u64> = (0..num_vals as u64).map(|i| i * 7 + (i % 5)).collect();
            create_and_validate::<BlockForCodec>(&vals, "block boundaries");
        }
    }

    #[test]
    fn test_block_for_outliers() {
        let mut vals: Vec<u64> = vec![42_000; 1000];
        vals[137] = u64::MAX / 2;
        vals[999] = 1 << 60;
        create_and_validate::<BlockForCodec>(&vals, "outliers");
    }

    #[test]
    fn test_block_for_all_widths() {
        // One block per width w: values in [0, 2^w).
        for w in 0..=64u32 {
            let max = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
            let vals: Vec<u64> = (0..200u64)
                .map(|i| (i.wrapping_mul(0x9E3779B97F4A7C15)) & max)
                .collect();
            create_and_validate::<BlockForCodec>(&vals, "widths");
        }
    }

    #[test]
    fn test_block_for_datasets() {
        let data_sets = crate::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BlockForCodec>(&data, name);
            data.reverse();
            create_and_validate::<BlockForCodec>(&data, name);
        }
    }

    #[test]
    fn test_block_for_wrapped_get_range() {
        use common::OwnedBytes;

        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };
        let vals: Vec<u64> = (0..5000u64).map(|i| i * 3 + (i % 17)).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::BlockFor], &mut buffer).unwrap();
        let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
        let mut out = vec![0u64; 3000];
        col.get_range(137, &mut out);
        assert_eq!(&vals[137..137 + 3000], &out[..]);
        let mut out = vec![0u64; 5000];
        col.get_range(0, &mut out);
        assert_eq!(&vals[..], &out[..]);
        // Small ranges go through the fused per-value path.
        let mut out = vec![0u64; 5];
        col.get_range(4990, &mut out);
        assert_eq!(&vals[4990..4995], &out[..]);
    }

    #[test]
    fn test_block_for_wrapped_get_range_f64() {
        use common::OwnedBytes;

        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };
        let vals: Vec<f64> = (0..3000u64).map(|i| (i as f64) * 0.25 - 100.0).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::BlockFor], &mut buffer).unwrap();
        let col = load_u64_based_column_values::<f64>(OwnedBytes::new(buffer)).unwrap();
        let mut out = vec![0f64; 2000];
        col.get_range(11, &mut out);
        assert_eq!(&vals[11..11 + 2000], &out[..]);
    }

    #[test]
    #[should_panic(expected = "get_range out of bounds")]
    fn test_block_for_get_range_rejects_padding() {
        let vals = [42u64];
        let mut buffer = Vec::new();
        let stats = {
            let mut collector = crate::column_values::u64_based::StatsCollector::default();
            collector.collect(42);
            collector.stats()
        };
        let estimator = BlockForEstimator::default();
        estimator
            .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
            .unwrap();
        let reader = BlockForCodec::load(OwnedBytes::new(buffer)).unwrap();
        let mut out = [0u64; 1];
        reader.get_range(1, &mut out);
    }

    fn stats_of(vals: &[u64]) -> ColumnStats {
        let mut collector = crate::column_values::u64_based::StatsCollector::default();
        for &val in vals {
            collector.collect(val);
        }
        collector.stats()
    }

    fn serialize_block_for(vals: &[u64]) -> (ColumnStats, Vec<u8>) {
        let stats = stats_of(vals);
        let mut buffer = Vec::new();
        BlockForEstimator::default()
            .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
            .unwrap();
        (stats, buffer)
    }

    #[test]
    fn test_block_for_footer_layout() {
        let mut vals: Vec<u64> = (0..128u64).collect();
        vals.extend(1000..1128u64);
        vals.extend(5000..5044u64);
        assert_eq!(vals.len(), 300);
        let (stats, buffer) = serialize_block_for(&vals);
        assert_eq!(stats.gcd.get(), 1);

        let total_units = 7 + 7 + 6usize;
        let offset_bits = compute_num_bits(total_units as u64);
        let width_bits = compute_num_bits(7);
        let min_bits = compute_num_bits(5000);
        assert_eq!((offset_bits, width_bits, min_bits), (5, 3, 13));

        // Head: 5 + 3 bits -> 1 byte. Minimum: 13 bits -> 2 bytes.
        let stride = RecordLayout::new(offset_bits, width_bits, min_bits).stride;
        assert_eq!(stride, 3);
        let footer_len = 3 * stride + FOOTER_PAD + 3;
        assert_eq!(
            buffer.len(),
            stats.num_bytes() as usize + 16 * total_units + footer_len + 4
        );
    }

    /// `estimate` decides which codec wins, so it has to predict the footer
    /// exactly, not approximately.
    #[test]
    fn test_block_for_estimate_matches_serialized_len() {
        let mut cases: Vec<(Vec<u64>, &str)> = vec![
            ((0..300u64).map(|i| i * 7 + (i % 5)).collect(), "ramp"),
            (vec![42_000; 1000], "constant"),
            ((0..128u64).collect(), "one block"),
            (vec![7], "single value"),
            (
                (0..5000u64)
                    .map(|i| i.wrapping_mul(2_654_435_761))
                    .collect(),
                "wide",
            ),
        ];

        // An outlier in one block: the block widths, and so the offset array, stop being uniform.
        let mut outliers = vec![42_000u64; 1000];
        outliers[137] = u64::MAX / 2;
        cases.push((outliers, "outliers"));

        for (vals, name) in cases {
            let stats = stats_of(&vals);
            let mut estimator = BlockForEstimator::default();
            for &val in &vals {
                estimator.collect(val);
            }
            estimator.finalize();
            let mut buffer = Vec::new();
            estimator
                .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
                .unwrap();
            assert_eq!(
                estimator.estimate(&stats),
                Some(buffer.len() as u64),
                "{name}: estimate must match the serialized length exactly"
            );
        }
    }

    /// Block minima are bitpacked at one column-wide width, so a column whose
    /// minima need all 64 bits drives the `BitUnpacker` width that is legal but
    /// skips the 8-byte fast path. Widths 57..=63 do not exist (`compute_num_bits`
    /// rounds them to 64), which is what keeps `BitUnpacker::new` from panicking.
    #[test]
    fn test_block_for_full_width_block_minima() {
        let vals: Vec<u64> = (0..1000u64)
            .map(|i| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) & !0xFFu64) | (i % 251))
            .collect();
        let (_, buffer) = serialize_block_for(&vals);
        let reader = BlockForCodec::load(OwnedBytes::new(buffer)).unwrap();
        for (i, &expected) in vals.iter().enumerate() {
            assert_eq!(reader.get_val(i as u32), expected, "row {i}");
        }
        let mut out = vec![0u64; vals.len()];
        reader.get_range(0, &mut out);
        assert_eq!(&out[..], &vals[..]);
    }
    #[test]
    fn test_block_for_many_blocks() {
        let vals: Vec<u64> = (0..200_000u64)
            .map(|i| i * 64 + (i.wrapping_mul(2_654_435_761) % (1 << (i / 4096 % 20))))
            .collect();
        let (_, buffer) = serialize_block_for(&vals);
        let reader = BlockForCodec::load(OwnedBytes::new(buffer)).unwrap();
        for i in (0..vals.len()).step_by(97) {
            assert_eq!(reader.get_val(i as u32), vals[i], "row {i}");
        }
        let mut out = vec![0u64; 4096];
        reader.get_range(150_003, &mut out);
        assert_eq!(&out[..], &vals[150_003..150_003 + 4096]);
    }

    #[test]
    fn test_block_for_rand() {
        for _ in 0..100 {
            let mut data = (0..1 + rand::random::<u16>() as usize % 2000)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BlockForCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<BlockForCodec>(&data, "rand");
        }
    }
}
