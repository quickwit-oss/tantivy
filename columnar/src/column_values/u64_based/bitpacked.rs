use std::io::{self, Write};

use common::{BinarySerializable, OwnedBytes};
use fastdivide::DividerU64;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator, Stats};
use crate::{ColumnValues, RowId};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    stats: Stats,
}

impl ColumnValues for BitpackedReader {
    #[inline(always)]
    fn get_val(&self, doc: u32) -> u64 {
        self.stats.min_value + self.stats.gcd * self.bit_unpacker.get(doc, &self.data)
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
}

fn num_bits(stats: &Stats) -> u8 {
    compute_num_bits(stats.amplitude() / stats.gcd)
}

#[derive(Default)]
pub struct BitpackedCodecEstimator;

impl ColumnCodecEstimator for BitpackedCodecEstimator {
    fn collect(&mut self, _value: u64) {}

    fn estimate(&self, stats: &Stats) -> Option<u64> {
        let num_bits_per_value = num_bits(stats);
        Some(stats.num_bytes() + (stats.num_rows as u64 * (num_bits_per_value as u64) + 7) / 8)
    }

    fn serialize(
        &self,
        stats: &Stats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        stats.serialize(wrt)?;
        let num_bits = num_bits(stats);
        let mut bit_packer = BitPacker::new();
        let divider = DividerU64::divide_by(stats.gcd);
        for val in vals {
            bit_packer.write(divider.divide(val - stats.min_value), num_bits, wrt)?;
        }
        bit_packer.close(wrt)?;
        Ok(())
    }
}

pub struct BitpackedCodec;

impl ColumnCodec for BitpackedCodec {
    type Reader = BitpackedReader;
    type Estimator = BitpackedCodecEstimator;

    /// Opens a fast field given a file.
    fn load(mut data: OwnedBytes) -> io::Result<Self::Reader> {
        let stats = Stats::deserialize(&mut data)?;
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
