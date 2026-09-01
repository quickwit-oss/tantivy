use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::{Range, RangeInclusive};

use common::{BinarySerializable, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::column_values::u64_based::{
    ColumnCodec, ColumnCodecEstimator, ColumnStats, MIN_BATCH_ROWS, get_range_per_value,
};
use crate::{ColumnValues, RowId};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    stats: ColumnStats,
}

#[inline(always)]
const fn div_ceil(n: u64, q: NonZeroU64) -> u64 {
    // copied from unstable rust standard library.
    let d = n / q.get();
    let r = n % q.get();
    if r > 0 { d + 1 } else { d }
}

// The bitpacked codec applies a linear transformation `f` over data that are bitpacked.
// f is defined by:
// f: bitpacked -> stats.min_value + stats.gcd * bitpacked
//
// In order to run range queries, we invert the transformation.
// `transform_range_before_linear_transformation` returns the range of values
// [min_bipacked_value..max_bitpacked_value] such that
// f(bitpacked) ∈ [min_value, max_value] <=> bitpacked ∈ [min_bitpacked_value, max_bitpacked_value]
fn transform_range_before_linear_transformation(
    stats: &ColumnStats,
    range: RangeInclusive<u64>,
) -> Option<RangeInclusive<u64>> {
    if range.is_empty() {
        return None;
    }
    let shifted_range =
        range.start().saturating_sub(stats.min_value)..=range.end().saturating_sub(stats.min_value);
    let start_before_gcd_multiplication: u64 = div_ceil(*shifted_range.start(), stats.gcd);
    let end_before_gcd_multiplication: u64 = *shifted_range.end() / stats.gcd;
    Some(start_before_gcd_multiplication..=end_before_gcd_multiplication)
}

impl BitpackedReader {
    /// Decodes rows `start..start + output.len()` through the batch kernels,
    /// with `min + gcd * val` applied in-register where the width allows.
    #[inline]
    fn decode_range(&self, start: u64, output: &mut [u64]) {
        assert!(
            start + output.len() as u64 <= self.stats.num_rows as u64,
            "Requested index is out of bounds."
        );
        self.bit_unpacker.get_batch_affine(
            start as usize,
            &self.data,
            output,
            self.stats.min_value,
            self.stats.gcd.get(),
        );
    }
}

impl ColumnValues for BitpackedReader {
    #[inline(always)]
    fn get_val(&self, doc: u32) -> u64 {
        self.stats.min_value + self.stats.gcd.get() * self.bit_unpacker.get(doc, &self.data)
    }

    #[inline]
    fn get_range(&self, start: u64, output: &mut [u64]) {
        if output.len() < MIN_BATCH_ROWS {
            get_range_per_value(self, start, output);
        } else {
            self.decode_range(start, output);
        }
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
        range: RangeInclusive<u64>,
        doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let Some(transformed_range) =
            transform_range_before_linear_transformation(&self.stats, range)
        else {
            positions.clear();
            return;
        };
        self.bit_unpacker.get_ids_for_value_range(
            transformed_range,
            doc_id_range,
            &self.data,
            positions,
        );
    }
}

fn num_bits(stats: &ColumnStats) -> u8 {
    compute_num_bits(stats.amplitude() / stats.gcd)
}

#[derive(Default)]
pub struct BitpackedCodecEstimator;

impl ColumnCodecEstimator for BitpackedCodecEstimator {
    fn collect(&mut self, _value: u64) {}

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        let num_bits_per_value = num_bits(stats);
        Some(stats.num_bytes() + (stats.num_rows as u64 * (num_bits_per_value as u64)).div_ceil(8))
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let num_bits = num_bits(stats);
        let mut bit_packer = BitPacker::new();
        let divider = DividerU64::divide_by(stats.gcd.get());
        for val in vals {
            bit_packer.write(divider.divide(val - stats.min_value), num_bits, wrt)?;
        }
        bit_packer.close(wrt)?;
        Ok(())
    }
}

pub struct BitpackedCodec;

impl ColumnCodec for BitpackedCodec {
    type ColumnValues = BitpackedReader;
    type Estimator = BitpackedCodecEstimator;

    /// Opens a fast field given a file.
    fn load(mut data: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut data)?;
        let num_bits = num_bits(&stats);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedReader {
            data,
            bit_unpacker,
            stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_with_codec_data_sets_simple() {
        create_and_validate::<BitpackedCodec>(&[4, 3, 12], "name");
    }

    #[test]
    fn test_with_codec_data_sets_simple_gcd() {
        create_and_validate::<BitpackedCodec>(&[1000, 2000, 3000], "name");
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = crate::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BitpackedCodec>(&data, name);
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, name);
        }
    }

    /// Every bit width, every offset class: batch `get_range` and `iter` must
    /// agree with the per-value `get_val` they replace.
    #[test]
    fn test_bitpacked_batch_decode_all_widths() {
        for w in 0..=64u32 {
            let max = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
            // Lengths around block boundaries: no full block, exact multiples,
            // and partial trailing blocks.
            for num_vals in [1usize, 127, 128, 129, 255, 256, 300, 512, 517] {
                let vals: Vec<u64> = (0..num_vals as u64)
                    .map(|i| i.wrapping_mul(0x9E3779B97F4A7C15) & max)
                    .collect();
                let mut buffer = Vec::new();
                let stats = {
                    let mut collector = crate::column_values::u64_based::StatsCollector::default();
                    for &v in &vals {
                        collector.collect(v);
                    }
                    collector.stats()
                };
                BitpackedCodecEstimator
                    .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
                    .unwrap();
                let reader = BitpackedCodec::load(OwnedBytes::new(buffer)).unwrap();

                let mut out = vec![0u64; num_vals];
                reader.get_range(0, &mut out);
                assert_eq!(out, vals, "get_range(0) w={w} n={num_vals}");

                // Every start offset in the first two blocks plus the tail,
                // so the entrance ramp and the partial-block exit both run.
                for &start in &[1usize, 7, 63, 127, 128, 129, 200] {
                    if start >= num_vals {
                        continue;
                    }
                    let mut out = vec![0u64; num_vals - start];
                    reader.get_range(start as u64, &mut out);
                    assert_eq!(out, vals[start..], "get_range({start}) w={w} n={num_vals}");
                    // Short ranges that end inside a full block.
                    let short = (num_vals - start).min(5);
                    let mut out = vec![0u64; short];
                    reader.get_range(start as u64, &mut out);
                    assert_eq!(
                        out,
                        vals[start..start + short],
                        "get_range({start}, {short}) w={w} n={num_vals}"
                    );
                }
            }
        }
    }

    /// The gcd transformation must survive the batch path, including the
    /// trailing partial block that stays per-value.
    #[test]
    fn test_bitpacked_batch_decode_gcd() {
        let vals: Vec<u64> = (0..1000u64).map(|i| 5_000 + i * 1_000).collect();
        let mut buffer = Vec::new();
        let stats = {
            let mut collector = crate::column_values::u64_based::StatsCollector::default();
            for &v in &vals {
                collector.collect(v);
            }
            collector.stats()
        };
        assert!(stats.gcd.get() > 1, "test needs a gcd column");
        BitpackedCodecEstimator
            .serialize(&stats, &mut vals.iter().copied(), &mut buffer)
            .unwrap();
        let reader = BitpackedCodec::load(OwnedBytes::new(buffer)).unwrap();
        let mut out = vec![0u64; vals.len()];
        reader.get_range(0, &mut out);
        assert_eq!(out, vals);
        // A range that starts in a full block and ends in the partial one.
        let mut out = vec![0u64; vals.len() - 900];
        reader.get_range(900, &mut out);
        assert_eq!(out, vals[900..]);
    }

    /// The monotonic wrapper forwards `get_range` on two paths (in-place when
    /// the mapping is type-preserving, buffered otherwise). Exercise both
    /// through the public load path.
    #[test]
    fn test_bitpacked_wrapped_get_range() {
        use crate::column_values::{
            CodecType, load_u64_based_column_values, serialize_u64_based_column_values,
        };

        let vals: Vec<u64> = (0..5000u64).map(|i| i * 3 + (i % 17)).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
        let mut out = vec![0u64; 3000];
        col.get_range(137, &mut out);
        assert_eq!(&vals[137..137 + 3000], &out[..]);
        let mut out = vec![0u64; 5000];
        col.get_range(0, &mut out);
        assert_eq!(&vals[..], &out[..]);
        // Ends inside the trailing partial block.
        let mut out = vec![0u64; 200];
        col.get_range(4800, &mut out);
        assert_eq!(&vals[4800..], &out[..]);
        // Short ranges go through the fused per-value path.
        let mut out = vec![0u64; 5];
        col.get_range(4990, &mut out);
        assert_eq!(&vals[4990..4995], &out[..]);

        // f64 takes the buffered path (Input u64 != Output f64).
        let vals: Vec<f64> = (0..3000u64).map(|i| (i as f64) * 0.25 - 100.0).collect();
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        let col = load_u64_based_column_values::<f64>(OwnedBytes::new(buffer)).unwrap();
        let mut out = vec![0f64; 2000];
        col.get_range(11, &mut out);
        assert_eq!(&vals[11..11 + 2000], &out[..]);
    }

    #[test]
    fn bitpacked_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BitpackedCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, "rand");
        }
    }
}
