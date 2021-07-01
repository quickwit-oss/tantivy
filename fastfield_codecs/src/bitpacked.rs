use crate::FastFieldCodecReader;
use crate::FastFieldCodecSerializer;
use crate::FastFieldDataAccess;
use crate::FastFieldStats;
use common::BinarySerializable;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use tantivy_bitpacker::BitUnpacker;

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedFastFieldReader {
    bit_unpacker: BitUnpacker,
    pub min_value_u64: u64,
    pub max_value_u64: u64,
}

impl<'data> FastFieldCodecReader for BitpackedFastFieldReader {
    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let (_data, mut footer) = bytes.split_at(bytes.len() - 16);
        let min_value = u64::deserialize(&mut footer)?;
        let amplitude = u64::deserialize(&mut footer)?;
        let max_value = min_value + amplitude;
        let num_bits = compute_num_bits(amplitude);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedFastFieldReader {
            min_value_u64: min_value,
            max_value_u64: max_value,
            bit_unpacker,
        })
    }
    #[inline]
    fn get_u64(&self, doc: u64, data: &[u8]) -> u64 {
        self.min_value_u64 + self.bit_unpacker.get(doc, data)
    }
    #[inline]
    fn min_value(&self) -> u64 {
        self.min_value_u64
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.max_value_u64
    }
}
pub struct BitpackedFastFieldSerializerLegacy<'a, W: 'a + Write> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
    amplitude: u64,
    num_bits: u8,
}

impl<'a, W: Write> BitpackedFastFieldSerializerLegacy<'a, W> {
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn open(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializerLegacy<'a, W>> {
        assert!(min_value <= max_value);
        let amplitude = max_value - min_value;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(BitpackedFastFieldSerializerLegacy {
            bit_packer,
            write,
            min_value,
            amplitude,
            num_bits,
        })
    }
    /// Pushes a new value to the currently open u64 fast field.
    #[inline]
    pub fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer
            .write(val_to_write, self.num_bits, &mut self.write)?;
        Ok(())
    }
    pub fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)?;
        self.min_value.serialize(&mut self.write)?;
        self.amplitude.serialize(&mut self.write)?;
        Ok(())
    }
}

pub struct BitpackedFastFieldSerializer {}

impl FastFieldCodecSerializer for BitpackedFastFieldSerializer {
    const NAME: &'static str = "Bitpacked";
    const ID: u8 = 1;
    /// Serializes data with the BitpackedFastFieldSerializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    fn serialize(
        write: &mut impl Write,
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        _data_iter1: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        let mut serializer =
            BitpackedFastFieldSerializerLegacy::open(write, stats.min_value, stats.max_value)?;

        for val in data_iter {
            serializer.add_val(val)?;
        }
        serializer.close_field()?;

        Ok(())
    }
    fn is_applicable(
        _fastfield_accessor: &impl FastFieldDataAccess,
        _stats: FastFieldStats,
    ) -> bool {
        true
    }
    fn estimate(_fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> f32 {
        let amplitude = stats.max_value - stats.min_value;
        let num_bits = compute_num_bits(amplitude);
        let num_bits_uncompressed = 64;
        num_bits as f32 / num_bits_uncompressed as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::get_codec_test_data_sets;

    fn create_and_validate(data: &[u64], name: &str) {
        crate::tests::create_and_validate::<BitpackedFastFieldSerializer, BitpackedFastFieldReader>(
            data, name,
        );
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = get_codec_test_data_sets();
        for (mut data, name) in data_sets {
            create_and_validate(&data, name);
            data.reverse();
            create_and_validate(&data, name);
        }
    }

    #[test]
    fn bitpacked_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate(&data, "rand");

            data.reverse();
            create_and_validate(&data, "rand");
        }
    }
}
