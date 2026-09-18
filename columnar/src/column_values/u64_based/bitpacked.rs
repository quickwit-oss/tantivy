use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use common::{BinarySerializable, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{BitPacker, BitUnpacker, compute_num_bits};

use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, ColumnStats};
use crate::{ColumnValues, MonotonicallyMappableToU64, RowId};

/// A bitpacked column reader. `u8::MAX` uses the bit width stored in the column.
#[derive(Clone)]
pub struct BitpackedReader<const NUM_BITS: u8 = { u8::MAX }> {
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

impl<const NUM_BITS: u8> BitpackedReader<NUM_BITS> {
    #[inline(always)]
    fn unpacker(&self) -> BitUnpacker {
        if NUM_BITS == u8::MAX {
            self.bit_unpacker
        } else {
            BitUnpacker::new(NUM_BITS)
        }
    }
}

impl<const NUM_BITS: u8> ColumnValues for BitpackedReader<NUM_BITS> {
    #[inline(always)]
    fn get_val(&self, doc: u32) -> u64 {
        self.stats.min_value + self.stats.gcd.get() * self.unpacker().get(doc, &self.data)
    }

    fn get_range(&self, start: u64, output: &mut [u64]) {
        debug_assert!(start <= u64::from(self.stats.num_rows));
        debug_assert!(output.len() as u64 <= u64::from(self.stats.num_rows) - start);
        if NUM_BITS == 0 {
            output.fill(self.stats.min_value);
            return;
        }
        self.unpacker().get_range(start as u32, &self.data, output);
        let skip_processing = self.stats.gcd.get() == 1 && self.stats.min_value == 0;
        if !skip_processing {
            for val in output {
                *val = self.stats.min_value + self.stats.gcd.get() * *val;
            }
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

/// Specialize widths with a meaningful decoding speedup; other widths share one decoder.
pub(super) fn load<T: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let reader = BitpackedCodec::load(bytes)?;
    macro_rules! specialize {
        ($($bits:literal),* $(,)?) => {
            match reader.bit_unpacker.bit_width() {
                $(
                    $bits => super::map_column_values::<_, T>(BitpackedReader::<$bits> {
                        data: reader.data,
                        bit_unpacker: reader.bit_unpacker,
                        stats: reader.stats,
                    }),
                )*
                _ => super::map_column_values::<_, T>(reader),
            }
        };
    }
    Ok(specialize!(1, 2, 3, 4, 5, 6, 7, 8, 16, 20, 24, 32, 64,))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_specialized_bit_widths() {
        for bits in (0..=56).chain(std::iter::once(64)) {
            let mask = u64::MAX.checked_shr(64 - bits as u32).unwrap_or(0);
            for gcd in [1, 3] {
                if gcd != 1 && mask > (u64::MAX - 7) / gcd {
                    continue;
                }
                let min_value = if bits == 64 { 0 } else { 7 };
                let vals: Vec<u64> = (0..257)
                    .map(|i| {
                        let packed = match i {
                            0 => 0,
                            1 => mask.min(1),
                            2 => mask,
                            _ => (i as u64).wrapping_mul(0x9e3779b97f4a7c15) & mask,
                        };
                        min_value + gcd * packed
                    })
                    .collect();
                let mut stats = super::super::StatsCollector::default();
                for &val in &vals {
                    stats.collect(val);
                }
                let mut buffer = Vec::new();
                BitpackedCodecEstimator
                    .serialize(&stats.stats(), &mut vals.iter().copied(), &mut buffer)
                    .unwrap();
                let data = OwnedBytes::new(buffer);
                let reader = load::<u64>(data.clone()).unwrap();
                let signed_reader = load::<i64>(data).unwrap();
                for start in 0..=vals.len() {
                    let mut output = vec![0; vals.len() - start];
                    reader.get_range(start as u64, &mut output);
                    assert_eq!(output, vals[start..]);
                    for (i, &val) in output.iter().enumerate() {
                        assert_eq!(reader.get_val((start + i) as u32), val);
                    }
                    let mut signed_output = vec![0; output.len()];
                    signed_reader.get_range(start as u64, &mut signed_output);
                    assert_eq!(
                        signed_output,
                        output.into_iter().map(i64::from_u64).collect::<Vec<_>>()
                    );
                }
            }
        }
    }

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
