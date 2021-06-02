use super::FastFieldDataAccess;
use super::FastFieldSerializerEstimate;
use super::FastFieldStats;
use crate::common::BinarySerializable;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference.
pub struct LinearInterpolFastFieldSerializer {}

impl LinearInterpolFastFieldSerializer {
    /// Creates a new fast field serializer.
    pub(crate) fn create(
        write: &mut impl Write,
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        data_iter1: impl Iterator<Item = u64>,
        data_iter2: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        assert!(stats.min_value <= stats.max_value);

        let step = (stats.max_value - stats.min_value) as f64 / (stats.num_vals as u64 - 1) as f64;
        // offset to ensure all values are positive
        let offset = data_iter1
            .enumerate()
            .map(|(pos, val)| {
                let calculated_value = stats.min_value + (pos as f64 * step) as u64;
                val as i64 - calculated_value as i64
            })
            .min()
            .unwrap()
            .abs() as u64;

        //calc new max
        let rel_max = data_iter2
            .enumerate()
            .map(|(pos, val)| {
                let calculated_value = stats.min_value + (pos as f64 * step) as u64;
                (val + offset) - calculated_value
            })
            .max()
            .unwrap();

        stats.min_value.serialize(write)?;
        let amplitude = rel_max;
        amplitude.serialize(write)?;
        offset.serialize(write)?;
        stats.min_value.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let mut bit_packer = BitPacker::new();
        for val in data_iter {
            bit_packer.write(val, num_bits, write)?;
        }
        bit_packer.close(write)?;

        Ok(())
    }
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
        let name = Self::codec_id().0;
        (ratio, name)
    }
    fn codec_id() -> (&'static str, u8) {
        ("LinearInterpol", 2)
    }
}
