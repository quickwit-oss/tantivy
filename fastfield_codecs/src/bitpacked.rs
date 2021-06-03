use crate::CodecId;
use crate::FastFieldDataAccess;
use crate::FastFieldSerializerEstimate;
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

impl<'data> BitpackedFastFieldReader {
    /// Opens a fast field given a file.
    pub fn open_from_bytes(bytes: &[u8]) -> io::Result<Self> {
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
    pub fn get_u64(&self, doc: u64, data: &[u8]) -> u64 {
        self.min_value_u64 + self.bit_unpacker.get(doc, &data)
    }
}
pub struct BitpackedFastFieldSerializer<'a, W: 'a + Write> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
    amplitude: u64,
    num_bits: u8,
}

impl<'a, W: Write> BitpackedFastFieldSerializer<'a, W> {
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
    ) -> io::Result<BitpackedFastFieldSerializer<'a, W>> {
        assert!(min_value <= max_value);
        let amplitude = max_value - min_value;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(BitpackedFastFieldSerializer {
            bit_packer,
            write,
            min_value,
            amplitude,
            num_bits,
        })
    }
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn create(
        write: &'a mut W,
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        let mut serializer = Self::open(write, stats.min_value, stats.max_value)?;

        for val in data_iter {
            serializer.add_val(val)?;
        }
        serializer.close_field()?;

        Ok(())
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

impl<'a, W: 'a + Write> FastFieldSerializerEstimate for BitpackedFastFieldSerializer<'a, W> {
    fn estimate(
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> (f32, &'static str) {
        let amplitude = stats.max_value - stats.min_value;
        let num_bits = compute_num_bits(amplitude);
        let num_bits_uncompressed = 64;
        let ratio = num_bits as f32 / num_bits_uncompressed as f32;
        let name = Self::NAME;
        (ratio, name)
    }
}
impl<'a, W: 'a + Write> CodecId for BitpackedFastFieldSerializer<'_, W> {
    const NAME: &'static str = "Bitpacked";
    const ID: u8 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_and_validate(data: &[u64]) {
        let mut out = vec![];
        BitpackedFastFieldSerializer::create(
            &mut out,
            &data,
            crate::tests::stats_from_vec(&data),
            data.iter().cloned(),
        )
        .unwrap();

        let reader = BitpackedFastFieldReader::open_from_bytes(&out).unwrap();
        for (doc, val) in data.iter().enumerate() {
            assert_eq!(reader.get_u64(doc as u64, &out), *val);
        }
    }

    #[test]
    fn bitpacked_fast_field_test_simple() {
        let data = (10..=20_u64).collect::<Vec<_>>();

        create_and_validate(&data);
    }

    #[test]
    fn bitpacked_fast_field_test_with_offset() {
        //let data = vec![5, 50, 95, 96, 97, 98, 99, 100];
        let mut data = vec![5, 6, 7, 8, 9, 10, 99, 100];
        create_and_validate(&data);

        data.reverse();
        create_and_validate(&data);
    }
    #[test]
    fn bitpacked_fast_field_test_no_structure() {
        let mut data = vec![5, 50, 3, 13, 1, 1000, 35];
        create_and_validate(&data);

        data.reverse();
        create_and_validate(&data);
    }
    #[test]
    fn bitpacked_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2 as u64)
                .collect::<Vec<_>>();
            create_and_validate(&data);

            data.reverse();
            create_and_validate(&data);
        }
    }
}
