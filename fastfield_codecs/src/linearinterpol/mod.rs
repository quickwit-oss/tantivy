mod serialize;

use common::BinarySerializable;
use std::io;
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitUnpacker;

use crate::FastFieldDataAccess;
/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearinterpolFastFieldReader<'data> {
    bytes: &'data [u8],
    bit_unpacker: BitUnpacker,
    pub first_value: u64,
    pub rel_max_value: u64,
    pub offset: u64,
    pub slope: f64,
}

impl<'data> LinearinterpolFastFieldReader<'data> {
    /// Opens a fast field given a file.
    pub fn open_from_bytes(mut bytes: &'data [u8]) -> io::Result<Self> {
        let rel_max_value = u64::deserialize(&mut bytes)?;
        let offset = u64::deserialize(&mut bytes)?;
        let first_value = u64::deserialize(&mut bytes)?;
        let last_value = u64::deserialize(&mut bytes)?;
        let num_vals = u64::deserialize(&mut bytes)?;
        let slope = (last_value as f64 - first_value as f64) / (num_vals as u64 - 1) as f64;

        let num_bits = compute_num_bits(rel_max_value);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(LinearinterpolFastFieldReader {
            bytes,
            first_value,
            rel_max_value,
            offset,
            bit_unpacker,
            slope,
        })
    }
    pub fn get_u64(&self, doc: u64) -> u64 {
        let calculated_value = self.first_value + (doc as f64 * self.slope) as u64;
        calculated_value + self.bit_unpacker.get(doc, &self.bytes) - self.offset

        //self.offset + self.min_value + self.bit_unpacker.get(doc, &self.bytes)
    }
}

impl<'a> FastFieldDataAccess for &'a [u64] {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}

impl FastFieldDataAccess for Vec<u64> {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn create_and_validate(data: &[u64]) -> (u64, u64) {
        let mut out = vec![];
        serialize::LinearInterpolFastFieldSerializer::create(
            &mut out,
            &data,
            crate::tests::stats_from_vec(&data),
            data.iter().cloned(),
            data.iter().cloned(),
            data.iter().cloned(),
        )
        .unwrap();

        let reader = LinearinterpolFastFieldReader::open_from_bytes(&out).unwrap();
        for (doc, val) in data.iter().enumerate() {
            assert_eq!(reader.get_u64(doc as u64), *val);
        }
        (reader.rel_max_value, reader.offset)
    }

    #[test]
    fn linear_interpol_fast_field_test_simple() {
        let data = (10..=20_u64).collect::<Vec<_>>();

        let (rel_max_value, offset) = create_and_validate(&data);

        assert_eq!(offset, 0);
        assert_eq!(rel_max_value, 0);
    }

    #[test]
    fn linear_interpol_fast_field_test_with_offset() {
        //let data = vec![5, 50, 95, 96, 97, 98, 99, 100];
        let mut data = vec![5, 6, 7, 8, 9, 10, 99, 100];
        create_and_validate(&data);

        data.reverse();
        create_and_validate(&data);
    }
    #[test]
    fn linear_interpol_fast_field_test_no_structure() {
        let mut data = vec![5, 50, 3, 13, 1, 1000, 35];
        create_and_validate(&data);

        data.reverse();
        create_and_validate(&data);
    }
    #[test]
    fn linear_interpol_fast_field_rand() {
        for i in 0..50000 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|num| rand::random::<i64>() as u64 / 2 as u64)
                .collect::<Vec<_>>();
            create_and_validate(&data);

            data.reverse();
            create_and_validate(&data);
        }
    }
}
