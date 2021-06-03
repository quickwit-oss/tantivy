use crate::CodecId;
use crate::FastFieldDataAccess;
use crate::FastFieldSerializerEstimate;
use crate::FastFieldStats;
use std::io::{self, Read, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use common::BinarySerializable;
use common::FixedSize;
use tantivy_bitpacker::BitUnpacker;

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearinterpolFastFieldReader {
    bit_unpacker: BitUnpacker,
    pub footer: LinearInterpolFooter,
    pub slope: f64,
}

#[derive(Clone, Debug)]
pub struct LinearInterpolFooter {
    pub relative_max_value: u64,
    pub offset: u64,
    pub first_val: u64,
    pub last_val: u64,
    pub num_vals: u64,
}

impl BinarySerializable for LinearInterpolFooter {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.relative_max_value.serialize(write)?;
        self.offset.serialize(write)?;
        self.first_val.serialize(write)?;
        self.last_val.serialize(write)?;
        self.num_vals.serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<LinearInterpolFooter> {
        Ok(LinearInterpolFooter {
            relative_max_value: u64::deserialize(reader)?,
            offset: u64::deserialize(reader)?,
            first_val: u64::deserialize(reader)?,
            last_val: u64::deserialize(reader)?,
            num_vals: u64::deserialize(reader)?,
        })
    }
}

impl FixedSize for LinearInterpolFooter {
    const SIZE_IN_BYTES: usize = 40;
}

impl LinearinterpolFastFieldReader {
    /// Opens a fast field given a file.
    pub fn open_from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let (_data, mut footer) = bytes.split_at(bytes.len() - LinearInterpolFooter::SIZE_IN_BYTES);
        let footer = LinearInterpolFooter::deserialize(&mut footer)?;
        //let rel_max_value = u64::deserialize(&mut footer)?;
        //let offset = u64::deserialize(&mut footer)?;
        //let first_value = u64::deserialize(&mut footer)?;
        //let last_value = u64::deserialize(&mut footer)?;
        //let num_vals = u64::deserialize(&mut footer)?;
        let slope = (footer.last_val as f64 - footer.first_val as f64)
            / (footer.num_vals as u64 - 1) as f64;

        let num_bits = compute_num_bits(footer.relative_max_value);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(LinearinterpolFastFieldReader {
            footer,
            bit_unpacker,
            slope,
        })
    }
    pub fn get_u64(&self, doc: u64, data: &[u8]) -> u64 {
        let calculated_value = self.footer.first_val + (doc as f64 * self.slope) as u64;
        (calculated_value + self.bit_unpacker.get(doc, &data)) - self.footer.offset
    }
}

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference bitpacked.
pub struct LinearInterpolFastFieldSerializer {}

// TODO not suitable if max is larger than i64::MAX / 2

impl LinearInterpolFastFieldSerializer {
    /// Creates a new fast field serializer.
    pub fn create(
        write: &mut impl Write,
        fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        data_iter1: impl Iterator<Item = u64>,
        data_iter2: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        assert!(stats.min_value <= stats.max_value);

        let first_val = fastfield_accessor.get(0);
        let last_val = fastfield_accessor.get(stats.num_vals as u32 - 1);
        let slope = get_slope(first_val, last_val, stats.num_vals);
        // todo walk over data just once and calulate offset on the fly
        // offset to ensure all values are positive
        let offset = data_iter1
            .enumerate()
            .map(|(pos, val)| {
                let calculated_value = first_val + (pos as f64 * slope) as u64;
                val as i64 - calculated_value as i64
            })
            .min()
            .unwrap()
            .abs() as u64;

        //calc new max
        let rel_max = data_iter2
            .enumerate()
            .map(|(pos, val)| {
                let calculated_value = first_val + (pos as f64 * slope) as u64;
                (val + offset) - calculated_value
            })
            .max()
            .unwrap();

        let amplitude = rel_max;
        let num_bits = compute_num_bits(amplitude);
        let mut bit_packer = BitPacker::new();
        for (pos, val) in data_iter.enumerate() {
            let calculated_value = first_val + (pos as f64 * slope) as u64;
            let diff = (val + offset) - calculated_value;
            bit_packer.write(diff, num_bits, write)?;
        }
        bit_packer.close(write)?;

        let footer = LinearInterpolFooter {
            relative_max_value: amplitude,
            offset,
            first_val,
            last_val,
            num_vals: stats.num_vals,
        };
        footer.serialize(write)?;
        Ok(())
    }
}
fn get_slope(first_val: u64, last_val: u64, num_vals: u64) -> f64 {
    (last_val as f64 - first_val as f64) / (num_vals as u64 - 1) as f64
}

impl FastFieldSerializerEstimate for LinearInterpolFastFieldSerializer {
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

impl CodecId for LinearInterpolFastFieldSerializer {
    const NAME: &'static str = "LinearInterpol";
    const ID: u8 = 2;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_and_validate(data: &[u64]) -> (u64, u64) {
        let mut out = vec![];
        LinearInterpolFastFieldSerializer::create(
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
            assert_eq!(reader.get_u64(doc as u64, &out), *val);
        }
        (reader.footer.relative_max_value, reader.footer.offset)
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
