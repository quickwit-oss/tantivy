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
//! Non-decimal columns self-detect (exception rate over the first blocks)
//! and opt out of selection; the size estimate is inflated so Alp only wins
//! with a real margin.
//!
//! The codec is not wired into [`super::CodecType`] yet -- it needs more
//! validation before columns serialize with it in production.
//!
//! ## Layout
//!
//! ```text
//! [ColumnStats]
//! [block 0 packed scaled values]...        // 16 * bit_width bytes each
//! [footer per block: exponent u8, bit_width u8, block_min VInt(zigzag),
//!                    num_exceptions u8, (position u8, raw u64 LE) * n]
//! [footer_len: u32 LE]
//! ```

use std::io::{self, Read, Write};
use std::sync::Arc;

use common::{
    BinarySerializable, CountingWriter, DeserializeFrom, OwnedBytes, VInt, f64_to_u64, u64_to_f64,
};
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::column_values::u64_based::block_decode::{
    BLOCK_LEN, BlockDecode, decode_block, iter_via_blocks,
    DecodeCost, min_batch_rows, partial_block_min_rows,
};
use crate::column_values::u64_based::block_for::vint_len;
use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::{ColumnValues, RowId};

/// Powers of ten tried as decimal scale (all exactly representable as f64).
const MAX_EXPONENT: usize = 13;
const POW10: [f64; MAX_EXPONENT + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
];

/// Kept well below 2^53 so integer<->f64 conversions stay exact.
const MAX_ABS_SCALED: f64 = (1i64 << 51) as f64;

/// Cost of one exception in bytes: position byte + raw value.
const EXCEPTION_COST: u64 = 9;

/// Estimate inflation: Alp is only picked with a >= 12.5% real size win.
const SELECTION_MARGIN_NUM: u64 = 9;
const SELECTION_MARGIN_DEN: u64 = 8;

/// After this many analyzed blocks, columns whose exception rate exceeds
/// [`DEAD_EXCEPTION_RATE`] stop being analyzed and opt out of selection.
const DEAD_DETECTION_BLOCKS: usize = 4;
const DEAD_EXCEPTION_RATE: f64 = 0.25;

#[inline]
fn zigzag_encode(val: i64) -> u64 {
    ((val << 1) ^ (val >> 63)) as u64
}

#[inline]
fn zigzag_decode(val: u64) -> i64 {
    ((val >> 1) as i64) ^ -((val & 1) as i64)
}

/// Encoding plan for one block.
struct BlockPlan {
    exponent: u8,
    bit_width: u8,
    /// Minimum scaled value among encodable values (0 if none).
    block_min: i64,
    /// Positions of values stored as raw exceptions.
    exceptions: Vec<u8>,
    /// Exact payload bytes: packed data + exception bytes + meta.
    cost: u64,
}

/// Returns the scaled integer if `mapped` round-trips at this exponent.
#[inline]
fn try_encode(mapped: u64, exponent: usize) -> Option<i64> {
    let val = u64_to_f64(mapped);
    let scaled_f = (val * POW10[exponent]).round();
    if scaled_f.is_nan() || scaled_f.abs() > MAX_ABS_SCALED {
        return None;
    }
    let scaled = scaled_f as i64;
    // Bit-exact round-trip check (catches -0.0, NaN payloads, precision).
    if f64_to_u64(scaled as f64 / POW10[exponent]) == mapped {
        Some(scaled)
    } else {
        None
    }
}

fn analyze_block(vals: &[u64]) -> BlockPlan {
    let mut best: Option<BlockPlan> = None;
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
            // Every value is an exception at this exponent.
            continue;
        }
        let bit_width = compute_num_bits((max_scaled - min_scaled) as u64);
        let cost = 16 * bit_width as u64
            + exceptions.len() as u64 * EXCEPTION_COST
            + 3
            + vint_len(zigzag_encode(min_scaled));
        if best.as_ref().map(|b| cost < b.cost).unwrap_or(true) {
            let no_exceptions = exceptions.is_empty();
            best = Some(BlockPlan {
                exponent: exponent as u8,
                bit_width,
                block_min: min_scaled,
                exceptions,
                cost,
            });
            // Larger exponents only widen the integers: once everything
            // round-trips, stop.
            if no_exceptions {
                break;
            }
        }
    }
    best.unwrap_or_else(|| BlockPlan {
        // All-exception block (e.g. non-float data).
        exponent: 0,
        bit_width: 0,
        block_min: 0,
        exceptions: (0..vals.len() as u8).collect(),
        cost: vals.len() as u64 * EXCEPTION_COST + 4,
    })
}

#[derive(Clone, Copy)]
struct AlpBlockMeta {
    start_byte_offset: u64,
    block_min: i64,
    /// `10^exponent`.
    scale: f64,
    /// Start of this block's exceptions in the flat raw-values array.
    exceptions_start: u32,
    /// Bitmap of exception positions within the block.
    exceptions_bitmap: [u64; 2],
    unpacker: BitUnpacker,
}

impl AlpBlockMeta {
    #[inline]
    fn exception_rank(&self, pos: u32) -> Option<u32> {
        let word = (pos >> 6) as usize;
        let bit = pos & 63;
        if (self.exceptions_bitmap[word] >> bit) & 1 == 0 {
            return None;
        }
        let mask = (1u64 << bit) - 1;
        let mut rank = (self.exceptions_bitmap[word] & mask).count_ones();
        if word == 1 {
            rank += self.exceptions_bitmap[0].count_ones();
        }
        Some(rank)
    }
}

#[derive(Clone)]
pub struct AlpReader {
    data: OwnedBytes,
    blocks: Arc<[AlpBlockMeta]>,
    /// Raw (mapped u64) exception values, in position order per block.
    exceptions: Arc<[u64]>,
    stats: ColumnStats,
}

impl BlockDecode for AlpReader {
    fn num_full_blocks(&self) -> usize {
        self.blocks.len()
    }

    fn partial_min_rows(&self, block_idx: usize) -> usize {
        partial_block_min_rows(self.blocks[block_idx].unpacker.bit_width())
    }

    fn decode_block_mapped(&self, block_idx: usize, out: &mut [u64; BLOCK_LEN]) {
        let meta = &self.blocks[block_idx];
        decode_block(
            meta.unpacker.bit_width(),
            &self.data[meta.start_byte_offset as usize..],
            out,
        );
        for slot in out.iter_mut() {
            let scaled = meta.block_min.wrapping_add(*slot as i64);
            *slot = f64_to_u64(scaled as f64 / meta.scale);
        }
        let mut exc_idx = meta.exceptions_start as usize;
        for word in 0..2 {
            let mut bits = meta.exceptions_bitmap[word];
            while bits != 0 {
                let pos = word * 64 + bits.trailing_zeros() as usize;
                out[pos] = self.exceptions[exc_idx];
                exc_idx += 1;
                bits &= bits - 1;
            }
        }
    }
}

impl ColumnValues for AlpReader {
    #[inline]
    fn get_val(&self, idx: u32) -> u64 {
        let meta = &self.blocks[idx as usize / BLOCK_LEN];
        let pos = idx % BLOCK_LEN as u32;
        if let Some(rank) = meta.exception_rank(pos) {
            return self.exceptions[meta.exceptions_start as usize + rank as usize];
        }
        let rel = meta
            .unpacker
            .get(pos, &self.data[meta.start_byte_offset as usize..]);
        let scaled = meta.block_min.wrapping_add(rel as i64);
        f64_to_u64(scaled as f64 / meta.scale)
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

    fn min_batch_rows(&self) -> usize {
        min_batch_rows(DecodeCost::Blocked)
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = u64> + 'a> {
        iter_via_blocks(self, self.stats.num_rows as usize)
    }
}

pub struct AlpEstimator {
    block: Vec<u64>,
    payload_bytes: u64,
    num_blocks: usize,
    num_exceptions: u64,
    num_values: u64,
    dead: bool,
}

impl Default for AlpEstimator {
    fn default() -> Self {
        AlpEstimator {
            block: Vec::with_capacity(BLOCK_LEN),
            payload_bytes: 0,
            num_blocks: 0,
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
        self.payload_bytes += plan.cost;
        self.num_blocks += 1;
        self.num_exceptions += plan.exceptions.len() as u64;
        self.num_values += self.block.len() as u64;
        self.block.clear();
        if self.num_blocks >= DEAD_DETECTION_BLOCKS
            && (self.num_exceptions as f64) > (self.num_values as f64) * DEAD_EXCEPTION_RATE
        {
            // Not decimal data: stop wasting cycles, opt out of selection.
            self.dead = true;
        }
    }
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
        let num_bytes = stats.num_bytes() + 4 + self.payload_bytes;
        Some(num_bytes * SELECTION_MARGIN_NUM / SELECTION_MARGIN_DEN)
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
        // (plan, raw exception values)
        let mut block_metas: Vec<(BlockPlan, Vec<u64>)> = Vec::new();
        let mut bit_packer = BitPacker::new();
        loop {
            block.clear();
            block.extend((&mut *vals).take(BLOCK_LEN));
            if block.is_empty() {
                break;
            }
            let plan = analyze_block(&block);
            let mut exception_values: Vec<u64> = Vec::with_capacity(plan.exceptions.len());
            let mut next_exception = 0usize;
            for (pos, &mapped) in block.iter().enumerate() {
                let is_exception = plan
                    .exceptions
                    .get(next_exception)
                    .map(|&p| p as usize == pos)
                    .unwrap_or(false);
                let rel = if is_exception {
                    next_exception += 1;
                    exception_values.push(mapped);
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
            bit_packer.flush(wrt)?;
            block_metas.push((plan, exception_values));
        }
        let mut counting_wrt = CountingWriter::wrap(wrt);
        for (plan, exception_values) in &block_metas {
            plan.exponent.serialize(&mut counting_wrt)?;
            plan.bit_width.serialize(&mut counting_wrt)?;
            VInt(zigzag_encode(plan.block_min)).serialize(&mut counting_wrt)?;
            (plan.exceptions.len() as u8).serialize(&mut counting_wrt)?;
            for (&pos, &raw) in plan.exceptions.iter().zip(exception_values) {
                pos.serialize(&mut counting_wrt)?;
                counting_wrt.write_all(&raw.to_le_bytes())?;
            }
        }
        let footer_len = u32::try_from(counting_wrt.written_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "column footer exceeds u32::MAX")
        })?;
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
        let (data, mut footer) = bytes.split(footer_offset);
        let num_blocks = (stats.num_rows as usize).div_ceil(BLOCK_LEN);
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut exceptions: Vec<u64> = Vec::new();
        let mut start_byte_offset = 0u64;
        for _ in 0..num_blocks {
            let exponent = u8::deserialize(&mut footer)?;
            let bit_width = u8::deserialize(&mut footer)?;
            let block_min = zigzag_decode(VInt::deserialize(&mut footer)?.0);
            let exceptions_start = exceptions.len() as u32;
            let num_exceptions = u8::deserialize(&mut footer)?;
            let mut bitmap = [0u64; 2];
            for _ in 0..num_exceptions {
                let pos = u8::deserialize(&mut footer)?;
                let mut raw = [0u8; 8];
                footer.read_exact(&mut raw)?;
                bitmap[(pos >> 6) as usize] |= 1u64 << (pos & 63);
                exceptions.push(u64::from_le_bytes(raw));
            }
            blocks.push(AlpBlockMeta {
                start_byte_offset,
                block_min,
                scale: POW10[(exponent as usize).min(MAX_EXPONENT)],
                exceptions_start,
                exceptions_bitmap: bitmap,
                unpacker: BitUnpacker::new(bit_width),
            });
            start_byte_offset += (BLOCK_LEN as u64 * bit_width as u64) / 8;
        }
        Ok(AlpReader {
            data,
            blocks: blocks.into(),
            exceptions: exceptions.into(),
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
        // the u8 position encoding and the u8 exception count.
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
}
