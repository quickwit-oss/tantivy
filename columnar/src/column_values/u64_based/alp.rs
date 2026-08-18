//! ALP (Adaptive Lossless Floating-Point) pseudo-decimal codec for f64 columns.
//!
//! f64 values arrive as monotonically mapped u64s (`common::f64_to_u64`),
//! which no integer codec can compress. Per 128-value block: find the
//! smallest power of ten so that `round(v * 10^e)` round-trips to the exact
//! original bits via `scaled as f64 / 10^e`; store the scaled integers
//! min-relative and bitpacked like `BlockFor`; keep non-round-tripping
//! values as raw 8-byte exceptions patched by position. Decoding divides
//! rather than multiplying by `10^-e`: the rounded division reproduces the
//! bits where e.g. `k * 0.01` is one ulp off.
//!
//! The codec is not wired into [`super::CodecType`] yet -- it needs more
//! validation before columns serialize with it in production.
//!
//! ## Layout
//!
//! ```text
//! [ColumnStats]
//! [block 0 packed scaled values][block 1 packed scaled values]...  // 16 * bit_width bytes each
//! [exception values: num_exceptions * u64 LE]        // flat, in block order
//! [exception positions: num_exceptions * u8]         // sorted within each block
//! [block records: num_blocks * stride bytes]         // (offset | bit_width | exponent | count,
//! [16 zero bytes]                                    //  exc_start, block_min zigzag)
//! [offset_bit_width: u8][width_bit_width: u8][exponent_bit_width: u8]
//! [count_bit_width: u8][exc_bit_width: u8][min_bit_width: u8]
//! [num_exceptions: u32 LE]
//! [footer_len: u32 LE]
//! ```
//!
//! The per-block exception count lives in the head word, so a block without
//! exceptions (and any column without exceptions, where the count and
//! `exc_start` fields are 0 bits wide) never touches the exception fields or
//! regions at read time.

use std::io::{self, Write};

use common::{
    BinarySerializable, CountingWriter, DeserializeFrom, OwnedBytes, f64_to_u64, u64_to_f64,
};
use tantivy_bitpacker::block_decode::decode_range;
use tantivy_bitpacker::{BitPacker, compute_num_bits};

use crate::column_values::u64_based::block_decode::{
    BLOCK_LEN, BatchThresholds, BlockDecode, DecodeCost, batch_thresholds, iter_via_blocks,
};
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::{ColumnValues, RowId};

const MAX_EXPONENT: usize = 13;
const POW10: [f64; MAX_EXPONENT + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
];

const MAX_ABS_SCALED: f64 = (1i64 << 51) as f64;
const EXCEPTION_COST: u64 = 9;
const FOOTER_PAD: usize = 16;
const FOOTER_TAIL: usize = 6 + 4;
const SELECTION_MARGIN_NUM: u64 = 9;
const SELECTION_MARGIN_DEN: u64 = 8;
const DEAD_DETECTION_BLOCKS: usize = 4;
const DEAD_EXCEPTION_RATE: f64 = 0.25;

#[inline(always)]
fn zigzag_encode(val: i64) -> u64 {
    ((val << 1) ^ (val >> 63)) as u64
}

#[inline(always)]
fn zigzag_decode(val: u64) -> i64 {
    ((val >> 1) as i64) ^ -((val & 1) as i64)
}

#[inline(always)]
fn low_mask(num_bits: u8) -> u64 {
    if num_bits >= 64 {
        u64::MAX
    } else {
        (1u64 << num_bits) - 1
    }
}

struct BlockPlan {
    exponent: u8,
    bit_width: u8,
    block_min: i64,
    exceptions: Vec<u8>,
}

#[inline(always)]
fn try_encode(mapped: u64, exponent: usize) -> Option<i64> {
    let val = u64_to_f64(mapped);
    let scaled_f = (val * POW10[exponent]).round();
    if scaled_f.is_nan() || scaled_f.abs() > MAX_ABS_SCALED {
        return None;
    }
    let scaled = scaled_f as i64;
    if f64_to_u64(scaled as f64 / POW10[exponent]) == mapped {
        Some(scaled)
    } else {
        None
    }
}

fn analyze_block(vals: &[u64]) -> BlockPlan {
    let mut best: Option<(BlockPlan, u64)> = None;
    for exponent in 0..=MAX_EXPONENT {
        let mut min_scaled = i64::MAX;
        let mut max_scaled = i64::MIN;
        let mut exceptions: Vec<u8> = Vec::new();
        for (pos, &mapped) in vals.iter().enumerate() {
            if let Some(scaled) = try_encode(mapped, exponent) {
                min_scaled = min_scaled.min(scaled);
                max_scaled = max_scaled.max(scaled);
            } else {
                exceptions.push(pos as u8);
            }
        }
        if min_scaled > max_scaled {
            continue;
        }
        let bit_width = compute_num_bits((max_scaled - min_scaled) as u64);
        let cost = 16 * bit_width as u64 + exceptions.len() as u64 * EXCEPTION_COST;
        if best.as_ref().map(|&(_, c)| cost < c).unwrap_or(true) {
            let no_exceptions = exceptions.is_empty();
            best = Some((
                BlockPlan {
                    exponent: exponent as u8,
                    bit_width,
                    block_min: min_scaled,
                    exceptions,
                },
                cost,
            ));

            if no_exceptions {
                break;
            }
        }
    }
    best.map(|(plan, _)| plan).unwrap_or_else(|| BlockPlan {
        // All-exception block (e.g. non-float data). The range is built over
        // usize before narrowing so all of 0..=127 survive for a full block.
        exponent: 0,
        bit_width: 0,
        block_min: 0,
        exceptions: (0..vals.len()).map(|pos| pos as u8).collect(),
    })
}

#[derive(Clone, Copy)]
struct BlockStat {
    bit_width: u8,
    exponent: u8,
    num_exceptions: u32,
    zigzag_min: u64,
}

impl BlockStat {
    fn of(plan: &BlockPlan) -> BlockStat {
        BlockStat {
            bit_width: plan.bit_width,
            exponent: plan.exponent,
            num_exceptions: plan.exceptions.len() as u32,
            zigzag_min: zigzag_encode(plan.block_min),
        }
    }
}

/// Column-wide bit width of each record field.
#[derive(Clone, Copy)]
struct FieldWidths {
    offset: u8,
    width: u8,
    exponent: u8,
    /// Per-block exception count. 0 for the common all-round-tripping column,
    /// so exception handling costs those columns nothing at read time.
    count: u8,
    exc: u8,
    min: u8,
}

impl FieldWidths {
    fn of(block_stats: &[BlockStat]) -> FieldWidths {
        let mut total_units = 0u64;
        let mut exc_start = 0u64;
        let mut max_width = 0u8;
        let mut max_exponent = 0u8;
        let mut max_count = 0u32;
        let mut max_exc_start = 0u64;
        let mut max_zigzag_min = 0u64;
        for stat in block_stats {
            max_width = max_width.max(stat.bit_width);
            max_exponent = max_exponent.max(stat.exponent);
            max_count = max_count.max(stat.num_exceptions);
            max_exc_start = max_exc_start.max(exc_start);
            max_zigzag_min = max_zigzag_min.max(stat.zigzag_min);
            total_units += stat.bit_width as u64;
            exc_start += stat.num_exceptions as u64;
        }
        FieldWidths {
            offset: compute_num_bits(total_units),
            width: compute_num_bits(max_width as u64),
            exponent: compute_num_bits(max_exponent as u64),
            count: compute_num_bits(max_count as u64),
            exc: compute_num_bits(max_exc_start),
            min: compute_num_bits(max_zigzag_min),
        }
    }
}

#[derive(Clone, Copy)]
struct RecordLayout {
    /// Bytes per record.
    stride: usize,
    offset_mask: u64,
    width_shift: u32,
    width_mask: u64,
    exponent_shift: u32,
    exponent_mask: u64,
    count_shift: u32,
    count_mask: u64,
    /// Byte offset of the exception start inside the record.
    exc_byte: usize,
    exc_mask: u64,
    /// Byte offset of the zigzag minimum inside the record.
    min_byte: usize,
    min_mask: u64,
}

impl RecordLayout {
    fn new(widths: FieldWidths) -> RecordLayout {
        // offset <= 31 bits (2^32 rows), width <= 7, exponent <= 4,
        // count <= 8: the head always fits one unaligned 8-byte load.
        let head_bytes = (widths.offset as usize
            + widths.width as usize
            + widths.exponent as usize
            + widths.count as usize)
            .div_ceil(8);
        let exc_bytes = (widths.exc as usize).div_ceil(8);
        RecordLayout {
            stride: head_bytes + exc_bytes + (widths.min as usize).div_ceil(8),
            offset_mask: low_mask(widths.offset),
            width_shift: widths.offset as u32,
            width_mask: low_mask(widths.width),
            exponent_shift: widths.offset as u32 + widths.width as u32,
            exponent_mask: low_mask(widths.exponent),
            count_shift: widths.offset as u32 + widths.width as u32 + widths.exponent as u32,
            count_mask: low_mask(widths.count),
            exc_byte: head_bytes,
            exc_mask: low_mask(widths.exc),
            min_byte: head_bytes + exc_bytes,
            min_mask: low_mask(widths.min),
        }
    }
}

#[derive(Clone, Copy)]
struct AlpBlockMeta {
    start_byte_offset: u64,
    block_min: i64,
    /// `10^exponent`.
    scale: f64,
    num_exceptions: u32,
    bit_width: u8,
}

#[derive(Clone)]
pub struct AlpReader {
    data: OwnedBytes,
    /// The records and everything after them.
    meta: OwnedBytes,
    layout: RecordLayout,
    num_blocks: usize,
    num_exceptions: u32,
    /// Byte offset of the flat exception value region in `data`.
    exc_values_start: usize,
    /// Byte offset of the flat exception position region in `data`.
    exc_positions_start: usize,
    stats: ColumnStats,
}

impl AlpReader {
    #[inline(always)]
    fn block_meta(&self, block_idx: usize) -> AlpBlockMeta {
        let record = block_idx * self.layout.stride;
        let head = u64::from_le_bytes(self.meta[record..record + 8].try_into().unwrap());
        let start_units = head & self.layout.offset_mask;
        let bit_width = (head >> self.layout.width_shift) & self.layout.width_mask;
        let exponent = (head >> self.layout.exponent_shift) & self.layout.exponent_mask;
        let num_exceptions = (head >> self.layout.count_shift) & self.layout.count_mask;
        let min_at = record + self.layout.min_byte;
        let zigzag_min = u64::from_le_bytes(self.meta[min_at..min_at + 8].try_into().unwrap())
            & self.layout.min_mask;
        AlpBlockMeta {
            start_byte_offset: start_units * 16,
            block_min: zigzag_decode(zigzag_min),
            scale: POW10[(exponent as usize).min(MAX_EXPONENT)],
            num_exceptions: num_exceptions as u32,
            bit_width: bit_width as u8,
        }
    }

    /// Start of this block's exceptions in the flat exception regions. Only
    /// read on the exception path: blocks without exceptions never touch it.
    #[inline(always)]
    fn exceptions_start(&self, block_idx: usize) -> u32 {
        let exc_at = block_idx * self.layout.stride + self.layout.exc_byte;
        (u64::from_le_bytes(self.meta[exc_at..exc_at + 8].try_into().unwrap())
            & self.layout.exc_mask) as u32
    }

    /// The sorted in-block positions of exceptions `start..end`.
    #[inline(always)]
    fn exception_positions(&self, start: u32, end: u32) -> &[u8] {
        let region = self.exc_positions_start;
        &self.data[region + start as usize..region + end as usize]
    }

    /// The raw (mapped u64) value of the column-wide exception `rank`.
    #[inline(always)]
    fn exception_value(&self, rank: u32) -> u64 {
        let at = self.exc_values_start + rank as usize * 8;
        u64::from_le_bytes(self.data[at..at + 8].try_into().unwrap())
    }

    /// The `bit_width`-bit slot `slot` of the block, still scaled and
    /// min-relative. Reading 8 bytes needs no bounds handling of its own
    /// because the exception regions and footer follow the block data.
    #[inline(always)]
    fn slot(&self, meta: &AlpBlockMeta, slot: u32) -> u64 {
        let bit = slot as usize * meta.bit_width as usize;
        let byte = meta.start_byte_offset as usize + bit / 8;
        let raw = u64::from_le_bytes(self.data[byte..byte + 8].try_into().unwrap());
        (raw >> (bit % 8)) & low_mask(meta.bit_width)
    }

    #[inline(always)]
    fn decode_slot(&self, meta: &AlpBlockMeta, pos: u32) -> u64 {
        let scaled = meta.block_min.wrapping_add(self.slot(meta, pos) as i64);
        f64_to_u64(scaled as f64 / meta.scale)
    }
}

impl BlockDecode for AlpReader {
    fn num_full_blocks(&self) -> usize {
        self.num_blocks
    }

    fn decode_block_range(&self, block_idx: usize, in_block: usize, out: &mut [u64]) {
        let meta = self.block_meta(block_idx);
        decode_range(
            meta.bit_width,
            in_block,
            &self.data[meta.start_byte_offset as usize..],
            out,
        );
        for slot in out.iter_mut() {
            let scaled = meta.block_min.wrapping_add(*slot as i64);
            *slot = f64_to_u64(scaled as f64 / meta.scale);
        }
        if meta.num_exceptions != 0 {
            let start = self.exceptions_start(block_idx);
            let end = start + meta.num_exceptions;
            let window = in_block..in_block + out.len();
            for (rank, &pos) in (start..end).zip(self.exception_positions(start, end)) {
                if window.contains(&(pos as usize)) {
                    out[pos as usize - in_block] = self.exception_value(rank);
                }
            }
        }
    }
}

impl ColumnValues for AlpReader {
    #[inline(always)]
    fn get_vals(&self, indexes: &[u32], out: &mut [u64]) {
        let mut i: usize = 0;
        while i < indexes.len() {
            let block_idx = indexes[i] as usize / BLOCK_LEN;
            let j = i + indexes[i..].partition_point(|&idx| idx as usize / BLOCK_LEN <= block_idx);
            let meta = self.block_meta(block_idx);
            if meta.num_exceptions == 0 {
                for (k, &idx) in indexes[i..j].iter().enumerate() {
                    out[i + k] = self.decode_slot(&meta, idx % BLOCK_LEN as u32);
                }
            } else {
                let start = self.exceptions_start(block_idx);
                let positions = self.exception_positions(start, start + meta.num_exceptions);
                for (k, &idx) in indexes[i..j].iter().enumerate() {
                    let pos = idx % BLOCK_LEN as u32;
                    out[i + k] = match positions.binary_search(&(pos as u8)) {
                        Ok(rank) => self.exception_value(start + rank as u32),
                        Err(_) => self.decode_slot(&meta, pos),
                    };
                }
            }
            i = j;
        }
    }

    #[inline]
    fn get_val(&self, idx: u32) -> u64 {
        let block_idx = idx as usize / BLOCK_LEN;
        let meta = self.block_meta(block_idx);
        let pos = idx % BLOCK_LEN as u32;
        if meta.num_exceptions != 0 {
            let start = self.exceptions_start(block_idx);
            let positions = self.exception_positions(start, start + meta.num_exceptions);
            if let Ok(rank) = positions.binary_search(&(pos as u8)) {
                return self.exception_value(start + rank as u32);
            }
        }
        self.decode_slot(&meta, pos)
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
        super::get_row_ids_for_value_range_batched(self, value_range, row_id_range, row_id_hits)
    }

    fn get_range(&self, start: u64, output: &mut [u64]) {
        assert!(
            start + output.len() as u64 <= u64::from(self.stats.num_rows),
            "get_range out of bounds"
        );
        self.decode_range(start, output);
    }

    fn batch_thresholds(&self) -> BatchThresholds {
        batch_thresholds(DecodeCost::Blocked)
    }

    /// Worth the `Box<dyn Iterator>` here where it is not for the flat codecs:
    /// `get_val` re-reads the block record and does a float divide per value.
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = u64> + 'a> {
        iter_via_blocks(self, self.stats.num_rows as usize)
    }
}

pub struct AlpEstimator {
    block: Vec<u64>,
    block_stats: Vec<BlockStat>,
    num_exceptions: u64,
    num_values: u64,
    dead: bool,
}

impl Default for AlpEstimator {
    fn default() -> Self {
        AlpEstimator {
            block: Vec::with_capacity(BLOCK_LEN),
            block_stats: Vec::new(),
            num_exceptions: 0,
            num_values: 0,
            dead: false,
        }
    }
}

impl AlpEstimator {
    fn flush_block(&mut self) {
        if self.block.is_empty() || self.dead {
            self.block.clear();
            return;
        }
        let plan = analyze_block(&self.block);
        self.num_exceptions += plan.exceptions.len() as u64;
        self.num_values += self.block.len() as u64;
        self.block_stats.push(BlockStat::of(&plan));
        self.block.clear();
        if self.block_stats.len() >= DEAD_DETECTION_BLOCKS
            && (self.num_exceptions as f64) > (self.num_values as f64) * DEAD_EXCEPTION_RATE
        {
            // Not decimal data: stop wasting cycles, opt out of selection.
            self.dead = true;
        }
    }
}

/// Exact serialized length of a column with these per-block stats.
fn serialized_len(stats: &ColumnStats, block_stats: &[BlockStat]) -> u64 {
    let widths = FieldWidths::of(block_stats);
    let total_units: u64 = block_stats.iter().map(|s| s.bit_width as u64).sum();
    let num_exceptions: u64 = block_stats.iter().map(|s| s.num_exceptions as u64).sum();
    stats.num_bytes()
        + 16 * total_units
        + num_exceptions * EXCEPTION_COST
        + block_stats.len() as u64 * RecordLayout::new(widths).stride as u64
        + (FOOTER_PAD + FOOTER_TAIL) as u64
        + 4
}

impl ColumnCodecEstimator for AlpEstimator {
    fn collect(&mut self, value: u64) {
        if self.dead {
            return;
        }
        self.block.push(value);
        if self.block.len() == BLOCK_LEN {
            self.flush_block();
        }
    }

    fn finalize(&mut self) {
        self.flush_block();
    }

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        if self.dead {
            return None;
        }
        Some(serialized_len(stats, &self.block_stats) * SELECTION_MARGIN_NUM / SELECTION_MARGIN_DEN)
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        assert!(!self.dead, "alp codec serialize called on a dead estimator");
        stats.serialize(wrt)?;
        let mut block: Vec<u64> = Vec::with_capacity(BLOCK_LEN);
        let mut block_stats: Vec<BlockStat> = Vec::new();
        let mut exc_values: Vec<u8> = Vec::new();
        let mut exc_positions: Vec<u8> = Vec::new();
        let mut bit_packer = BitPacker::new();
        loop {
            block.clear();
            block.extend((&mut *vals).take(BLOCK_LEN));
            if block.is_empty() {
                break;
            }
            let plan = analyze_block(&block);
            let mut next_exception = 0usize;
            for (pos, &mapped) in block.iter().enumerate() {
                let is_exception = plan
                    .exceptions
                    .get(next_exception)
                    .map(|&p| p as usize == pos)
                    .unwrap_or(false);
                let rel = if is_exception {
                    next_exception += 1;
                    exc_values.extend_from_slice(&mapped.to_le_bytes());
                    exc_positions.push(pos as u8);
                    0u64
                } else {
                    let scaled = try_encode(mapped, plan.exponent as usize)
                        .expect("value must round-trip according to the block plan");
                    (scaled - plan.block_min) as u64
                };
                bit_packer.write(rel, plan.bit_width, wrt)?;
            }
            for _ in block.len()..BLOCK_LEN {
                bit_packer.write(0u64, plan.bit_width, wrt)?;
            }
            // 128 * bit_width bits is a multiple of 64: the packer is empty.
            bit_packer.flush(wrt)?;
            block_stats.push(BlockStat::of(&plan));
        }
        wrt.write_all(&exc_values)?;
        wrt.write_all(&exc_positions)?;

        let widths = FieldWidths::of(&block_stats);
        let layout = RecordLayout::new(widths);
        let head_bytes = layout.exc_byte;
        let exc_bytes = layout.min_byte - layout.exc_byte;
        let min_bytes = layout.stride - layout.min_byte;
        let mut counting_wrt = CountingWriter::wrap(wrt);
        let mut units = 0u64;
        let mut exc_start = 0u64;
        for stat in &block_stats {
            let head = units
                | ((stat.bit_width as u64) << layout.width_shift)
                | ((stat.exponent as u64) << layout.exponent_shift)
                | ((stat.num_exceptions as u64) << layout.count_shift);
            counting_wrt.write_all(&head.to_le_bytes()[..head_bytes])?;
            counting_wrt.write_all(&exc_start.to_le_bytes()[..exc_bytes])?;
            counting_wrt.write_all(&stat.zigzag_min.to_le_bytes()[..min_bytes])?;
            units += stat.bit_width as u64;
            exc_start += stat.num_exceptions as u64;
        }
        counting_wrt.write_all(&[0u8; FOOTER_PAD])?;
        widths.offset.serialize(&mut counting_wrt)?;
        widths.width.serialize(&mut counting_wrt)?;
        widths.exponent.serialize(&mut counting_wrt)?;
        widths.count.serialize(&mut counting_wrt)?;
        widths.exc.serialize(&mut counting_wrt)?;
        widths.min.serialize(&mut counting_wrt)?;
        u32::try_from(exc_start)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many alp exceptions"))?
            .serialize(&mut counting_wrt)?;
        let footer_len = u32::try_from(counting_wrt.written_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "column footer exceeds u32::MAX")
        })?;
        debug_assert_eq!(
            u64::from(footer_len),
            block_stats.len() as u64 * layout.stride as u64 + (FOOTER_PAD + FOOTER_TAIL) as u64
        );
        footer_len.serialize(&mut counting_wrt)?;
        Ok(())
    }
}

pub struct AlpCodec;

impl ColumnCodec for AlpCodec {
    type ColumnValues = AlpReader;
    type Estimator = AlpEstimator;

    fn load(mut bytes: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut bytes)?;
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;

        let data = bytes.clone();
        let meta = bytes.slice(footer_offset..bytes.len());
        let tail = meta.len();
        let num_exceptions: u32 = (&meta[tail - 8..tail - 4]).deserialize()?;
        let widths = FieldWidths {
            offset: meta[tail - 14],
            width: meta[tail - 13],
            exponent: meta[tail - 12],
            count: meta[tail - 11],
            exc: meta[tail - 10],
            min: meta[tail - 9],
        };
        let exc_positions_start = footer_offset - num_exceptions as usize;
        let exc_values_start = exc_positions_start - num_exceptions as usize * 8;

        Ok(AlpReader {
            data,
            meta,
            layout: RecordLayout::new(widths),
            num_blocks: (stats.num_rows as usize).div_ceil(BLOCK_LEN),
            num_exceptions,
            exc_values_start,
            exc_positions_start,
            stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::*;
    use crate::MonotonicallyMappableToU64;
    use crate::column_values::u64_based::{ColumnCodec, StatsCollector};

    /// Serializes through the estimator directly: the codec is not wired into
    /// `CodecType`, so the public selection path cannot reach it.
    fn serialize_alp(vals: &[u64]) -> Option<Vec<u8>> {
        let mut stats_collector = StatsCollector::default();
        let mut estimator = AlpEstimator::default();
        for &val in vals {
            stats_collector.collect(val);
            estimator.collect(val);
        }
        estimator.finalize();
        let stats = stats_collector.stats();
        estimator.estimate(&stats)?;
        let mut buffer = Vec::new();
        estimator
            .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
            .unwrap();
        Some(buffer)
    }

    fn serialize_and_check(f64_vals: &[f64]) -> Option<f64> {
        let vals: Vec<u64> = f64_vals.iter().map(|&v| v.to_u64()).collect();
        let buffer = serialize_alp(&vals)?;
        let bits_per_value = buffer.len() as f64 * 8.0 / vals.len().max(1) as f64;
        let col = AlpCodec::load(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals() as usize, vals.len());
        for (idx, &expected) in vals.iter().enumerate() {
            assert_eq!(col.get_val(idx as u32), expected, "mismatch at {idx}");
        }
        let indexes: Vec<u32> = (0..vals.len() as u32).step_by(3).collect();
        let mut out = vec![0u64; indexes.len()];
        col.get_vals(&indexes, &mut out);
        for (k, &idx) in indexes.iter().enumerate() {
            assert_eq!(out[k], vals[idx as usize], "get_vals mismatch at {idx}");
        }
        let mut out = vec![0u64; vals.len()];
        col.get_range(0, &mut out);
        assert_eq!(out, vals, "get_range mismatch");
        if vals.len() > 100 {
            let mut out = vec![0u64; vals.len() - 63];
            col.get_range(63, &mut out);
            assert_eq!(&out[..], &vals[63..], "unaligned get_range mismatch");
        }
        let iter_vals: Vec<u64> = col.iter().collect();
        assert_eq!(iter_vals, vals, "iter mismatch");
        Some(bits_per_value)
    }

    #[test]
    fn test_alp_two_decimals() {
        let vals: Vec<f64> = (0..5000u64)
            .map(|i| ((i * 37) % 1_000_000) as f64 / 100.0)
            .collect();
        let bits = serialize_and_check(&vals).unwrap();
        assert!(bits < 30.0, "expected < 30 bits/value, got {bits}");
    }

    #[test]
    fn test_alp_special_values() {
        let vals = vec![
            0.0,
            -0.0,
            1.25,
            -3.75,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            1e300,
            -1e-300,
            42.42,
        ];
        serialize_and_check(&vals).unwrap();
    }

    #[test]
    fn test_alp_block_boundaries() {
        for num_vals in [1usize, 127, 128, 129, 256, 1027] {
            let vals: Vec<f64> = (0..num_vals)
                .map(|i| (i as f64 * 5.0 - 300.0) / 100.0)
                .collect();
            serialize_and_check(&vals).unwrap();
        }
    }

    #[test]
    fn test_alp_mixed_exceptions() {
        // 90% two-decimal values, 10% arbitrary doubles.
        let vals: Vec<f64> = (0..3000usize)
            .map(|i| {
                if i % 10 == 3 {
                    (i as f64).sqrt() * std::f64::consts::PI
                } else {
                    (i % 100_000) as f64 / 100.0
                }
            })
            .collect();
        let bits = serialize_and_check(&vals).unwrap();
        assert!(bits < 40.0, "expected < 40 bits/value, got {bits}");
    }

    #[test]
    fn test_alp_full_block_of_exceptions() {
        // A whole 128-value block where no exponent round-trips, inside a
        // column that is otherwise plain two-decimal data. The block falls
        // back to the all-exception plan, whose 128 positions must survive
        // the u8 position encoding and the exception-count derivation.
        let vals: Vec<f64> = (0..1024usize)
            .map(|i| {
                if (640..768).contains(&i) {
                    f64::NAN
                } else {
                    (i % 100_000) as f64 / 100.0
                }
            })
            .collect();
        serialize_and_check(&vals).unwrap();
    }

    /// `decode_block_range` patches exceptions by their *block-relative*
    /// position into an output that starts at `in_block`, so every start and
    /// length must land the patch on the same row `get_val` does. Exceptions
    /// are placed to hit the first and last slot of a block, both ramps of the
    /// unpacker, and the short chunks that fall back to `get_val`.
    #[test]
    fn test_alp_get_range_exception_windows() {
        let vals: Vec<f64> = (0..1024usize)
            .map(|i| {
                let in_block = i % BLOCK_LEN;
                if matches!(in_block, 0 | 1 | 7 | 8 | 63 | 120 | 126 | 127) {
                    (i as f64).sqrt() * std::f64::consts::PI
                } else {
                    (i % 100_000) as f64 / 100.0
                }
            })
            .collect();
        let mapped: Vec<u64> = vals.iter().map(|&v| f64_to_u64(v)).collect();
        let bytes = serialize_alp(&mapped).expect("alp should take this column");
        let col = AlpCodec::load(OwnedBytes::new(bytes)).unwrap();

        for start in 0..(2 * BLOCK_LEN + 9) {
            for len in [1usize, 7, 8, 15, 16, 17, 64, 128, 129, 200] {
                if start + len > mapped.len() {
                    continue;
                }
                let mut out = vec![0u64; len];
                col.get_range(start as u64, &mut out);
                assert_eq!(
                    &out[..],
                    &mapped[start..start + len],
                    "get_range({start}, {len}) mismatch"
                );
            }
        }
    }

    #[test]
    fn test_alp_rejects_non_decimal_column() {
        // Integer-mapped values look like garbage floats: the estimator must
        // detect this and opt out.
        let vals: Vec<u64> = (0..5000u64).map(|i| i * 12345 + 7).collect();
        assert!(
            serialize_alp(&vals).is_none(),
            "alp must opt out of non-decimal columns"
        );
    }

    #[test]
    fn test_alp_rejects_full_precision_floats() {
        let mut state = 0x9E3779B97F4A7C15u64;
        let vals: Vec<f64> = (0..5000)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                (state >> 11) as f64 / (1u64 << 53) as f64 + 0.123456789123456
            })
            .collect();
        let mapped: Vec<u64> = vals.iter().map(|&v| v.to_u64()).collect();
        assert!(
            serialize_alp(&mapped).is_none(),
            "alp must opt out of full-precision floats"
        );
    }

    #[test]
    #[should_panic(expected = "get_range out of bounds")]
    fn test_alp_get_range_rejects_padding() {
        let vals: Vec<u64> = (0..5u64).map(|i| (i as f64 / 100.0).to_u64()).collect();
        let buffer = serialize_alp(&vals).unwrap();
        let reader = AlpCodec::load(OwnedBytes::new(buffer)).unwrap();
        let mut out = [0u64; 2];
        reader.get_range(4, &mut out);
    }

    #[test]
    fn test_alp_empty() {
        let vals: Vec<f64> = Vec::new();
        serialize_and_check(&vals);
    }

    fn stats_of(vals: &[u64]) -> ColumnStats {
        let mut collector = StatsCollector::default();
        for &val in vals {
            collector.collect(val);
        }
        collector.stats()
    }

    #[test]
    fn test_alp_footer_layout() {
        // 3 blocks of two-decimal data: scaled amplitudes 127, 127, 43 at
        // exponent 2, no exceptions.
        let vals: Vec<u64> = (0..300u64).map(|i| (i as f64 / 100.0).to_u64()).collect();
        let buffer = serialize_alp(&vals).unwrap();
        let stats = stats_of(&vals);

        let total_units = 7 + 7 + 6usize;
        // offset: num_bits(20) = 5, width: num_bits(7) = 3, exponent:
        // num_bits(2) = 2, count/exc_start: 0 bits -> head 10 bits -> 2
        // bytes, no exception bytes. Minima scaled: 0, 12800, 25600;
        // zigzag(25600) = 51200 -> 16 bits -> 2 bytes.
        let widths = FieldWidths {
            offset: 5,
            width: 3,
            exponent: 2,
            count: 0,
            exc: 0,
            min: 16,
        };
        let stride = RecordLayout::new(widths).stride;
        assert_eq!(stride, 4);
        let footer_len = 3 * stride + FOOTER_PAD + 6 + 4;
        assert_eq!(
            buffer.len(),
            stats.num_bytes() as usize + 16 * total_units + footer_len + 4
        );
    }

    /// The estimate is the exact serialized length inflated by the selection
    /// margin, so codec selection sees real bytes.
    #[test]
    fn test_alp_estimate_matches_serialized_len() {
        let cases: Vec<(Vec<f64>, &str)> = vec![
            ((0..300).map(|i| i as f64 / 100.0).collect(), "ramp"),
            (vec![42.42; 1000], "constant"),
            ((0..128).map(|i| i as f64 * 0.25).collect(), "one block"),
            (vec![7.5], "single value"),
            (
                (0..3000usize)
                    .map(|i| {
                        if i % 10 == 3 {
                            (i as f64).sqrt() * std::f64::consts::PI
                        } else {
                            (i % 100_000) as f64 / 100.0
                        }
                    })
                    .collect(),
                "mixed exceptions",
            ),
        ];
        for (f64_vals, name) in cases {
            let vals: Vec<u64> = f64_vals.iter().map(|&v| v.to_u64()).collect();
            let stats = stats_of(&vals);
            let mut estimator = AlpEstimator::default();
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
                Some(buffer.len() as u64 * SELECTION_MARGIN_NUM / SELECTION_MARGIN_DEN),
                "{name}: estimate must be the serialized length times the selection margin"
            );
        }
    }
}
