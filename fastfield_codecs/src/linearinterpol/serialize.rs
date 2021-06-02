use crate::FastFieldDataAccess;
use crate::FastFieldSerializerEstimate;
use crate::FastFieldStats;
use common::BinarySerializable;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference.
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

        //let first_val = stats.min_value;
        //let last_val = stats.max_value;
        let first_val = fastfield_accessor.get(0);
        let last_val = fastfield_accessor.get(stats.num_vals as u32 - 1);
        let slope = (last_val as f64 - first_val as f64) / (stats.num_vals as u64 - 1) as f64;
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
        amplitude.serialize(write)?;
        offset.serialize(write)?;
        first_val.serialize(write)?;
        last_val.serialize(write)?;
        stats.num_vals.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let mut bit_packer = BitPacker::new();
        for (pos, val) in data_iter.enumerate() {
            let calculated_value = first_val + (pos as f64 * slope) as u64;
            let diff = (val + offset) - calculated_value;
            bit_packer.write(diff, num_bits, write)?;
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
