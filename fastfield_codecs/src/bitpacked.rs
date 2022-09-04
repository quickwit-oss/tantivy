use std::io::{self, Write};

use common::BinarySerializable;
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::{Column, FastFieldCodec, FastFieldCodecType};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    min_value_u64: u64,
    max_value_u64: u64,
    num_vals: u64,
}

impl Column for BitpackedReader {
    #[inline]
    fn get_val(&self, doc: u64) -> u64 {
        self.min_value_u64 + self.bit_unpacker.get(doc, &self.data)
    }
    #[inline]
    fn min_value(&self) -> u64 {
        self.min_value_u64
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.max_value_u64
    }
    #[inline]
    fn num_vals(&self) -> u64 {
        self.num_vals
    }
}
pub struct BitpackedSerializerLegacy<'a, W: 'a + Write> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
    num_vals: u64,
    amplitude: u64,
    num_bits: u8,
}

impl<'a, W: Write> BitpackedSerializerLegacy<'a, W> {
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
    ) -> io::Result<BitpackedSerializerLegacy<'a, W>> {
        assert!(min_value <= max_value);
        let amplitude = max_value - min_value;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(BitpackedSerializerLegacy {
            bit_packer,
            write,
            min_value,
            num_vals: 0,
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
        self.num_vals += 1;
        Ok(())
    }
    pub fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)?;
        self.min_value.serialize(&mut self.write)?;
        self.amplitude.serialize(&mut self.write)?;
        self.num_vals.serialize(&mut self.write)?;
        Ok(())
    }
}

pub struct BitpackedCodec;

impl FastFieldCodec for BitpackedCodec {
    /// The CODEC_TYPE is an enum value used for serialization.
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::Bitpacked;

    type Reader = BitpackedReader;

    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self::Reader> {
        let footer_offset = bytes.len() - 24;
        let (data, mut footer) = bytes.split(footer_offset);
        let min_value = u64::deserialize(&mut footer)?;
        let amplitude = u64::deserialize(&mut footer)?;
        let num_vals = u64::deserialize(&mut footer)?;
        let max_value = min_value + amplitude;
        let num_bits = compute_num_bits(amplitude);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedReader {
            data,
            bit_unpacker,
            min_value_u64: min_value,
            max_value_u64: max_value,
            num_vals,
        })
    }

    /// Serializes data with the BitpackedFastFieldSerializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    fn serialize(write: &mut impl Write, fastfield_accessor: &dyn Column) -> io::Result<()> {
        let mut serializer = BitpackedSerializerLegacy::open(
            write,
            fastfield_accessor.min_value(),
            fastfield_accessor.max_value(),
        )?;

        for val in fastfield_accessor.iter() {
            serializer.add_val(val)?;
        }
        serializer.close_field()?;

        Ok(())
    }

    fn estimate(fastfield_accessor: &impl Column) -> Option<f32> {
        let amplitude = fastfield_accessor.max_value() - fastfield_accessor.min_value();
        let num_bits = compute_num_bits(amplitude);
        let num_bits_uncompressed = 64;
        Some(num_bits as f32 / num_bits_uncompressed as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::get_codec_test_datasets;

    fn create_and_validate(data: &[u64], name: &str) {
        crate::tests::create_and_validate::<BitpackedCodec>(data, name);
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = get_codec_test_datasets();
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
