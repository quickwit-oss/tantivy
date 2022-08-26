use std::io::{self, Read, Write};
use std::ops::Sub;

use common::{BinarySerializable, FixedSize};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::{
    FastFieldCodecDeserializer, FastFieldCodecSerializer, FastFieldCodecType, FastFieldDataAccess,
};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    pub footer: LinearFooter,
    pub slope: f32,
}

#[derive(Clone, Debug)]
pub struct LinearFooter {
    pub relative_max_value: u64,
    pub offset: u64,
    pub first_val: u64,
    pub last_val: u64,
    pub num_vals: u64,
    pub min_value: u64,
    pub max_value: u64,
}

impl BinarySerializable for LinearFooter {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.relative_max_value.serialize(write)?;
        self.offset.serialize(write)?;
        self.first_val.serialize(write)?;
        self.last_val.serialize(write)?;
        self.num_vals.serialize(write)?;
        self.min_value.serialize(write)?;
        self.max_value.serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<LinearFooter> {
        Ok(LinearFooter {
            relative_max_value: u64::deserialize(reader)?,
            offset: u64::deserialize(reader)?,
            first_val: u64::deserialize(reader)?,
            last_val: u64::deserialize(reader)?,
            num_vals: u64::deserialize(reader)?,
            min_value: u64::deserialize(reader)?,
            max_value: u64::deserialize(reader)?,
        })
    }
}

impl FixedSize for LinearFooter {
    const SIZE_IN_BYTES: usize = 56;
}

impl FastFieldCodecDeserializer for LinearReader {
    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self> {
        let footer_offset = bytes.len() - LinearFooter::SIZE_IN_BYTES;
        let (data, mut footer) = bytes.split(footer_offset);
        let footer = LinearFooter::deserialize(&mut footer)?;
        let slope = get_slope(footer.first_val, footer.last_val, footer.num_vals);
        let num_bits = compute_num_bits(footer.relative_max_value);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(LinearReader {
            data,
            bit_unpacker,
            footer,
            slope,
        })
    }
}

impl FastFieldDataAccess for LinearReader {
    #[inline]
    fn get_val(&self, doc: u64) -> u64 {
        let calculated_value = get_calculated_value(self.footer.first_val, doc, self.slope);
        (calculated_value + self.bit_unpacker.get(doc, &self.data)) - self.footer.offset
    }

    #[inline]
    fn min_value(&self) -> u64 {
        self.footer.min_value
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.footer.max_value
    }
    #[inline]
    fn num_vals(&self) -> u64 {
        self.footer.num_vals
    }
}

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference bitpacked.
pub struct LinearSerializer {}

#[inline]
pub(crate) fn get_slope(first_val: u64, last_val: u64, num_vals: u64) -> f32 {
    if num_vals <= 1 {
        return 0.0;
    }
    //  We calculate the slope with f64 high precision and use the result in lower precision f32
    //  This is done in order to handle estimations for very large values like i64::MAX
    let diff = diff(last_val, first_val);
    (diff / (num_vals - 1) as f64) as f32
}

/// Delay the cast, to improve precision for very large u64 values.
///
/// Since i64 is mapped monotonically to u64 space, 0i64 is after the mapping i64::MAX.
/// So very large values are not uncommon.
///
/// ```rust
///     let val1 = i64::MAX;
///     let val2 = i64::MAX - 100;
///     assert_eq!(val1 - val2, 100);
///     assert_eq!(val1 as f64 - val2 as f64, 0.0);
/// ```
fn diff(val1: u64, val2: u64) -> f64 {
    if val1 >= val2 {
        (val1 - val2) as f64
    } else {
        (val2 - val1) as f64 * -1.0
    }
}

#[inline]
pub fn get_calculated_value(first_val: u64, pos: u64, slope: f32) -> u64 {
    if slope < 0.0 {
        first_val - (pos as f32 * -slope) as u64
    } else {
        first_val + (pos as f32 * slope) as u64
    }
}

impl FastFieldCodecSerializer for LinearSerializer {
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::Linear;

    /// Creates a new fast field serializer.
    fn serialize(
        write: &mut impl Write,
        fastfield_accessor: &dyn FastFieldDataAccess,
    ) -> io::Result<()> {
        assert!(fastfield_accessor.min_value() <= fastfield_accessor.max_value());

        let first_val = fastfield_accessor.get_val(0);
        let last_val = fastfield_accessor.get_val(fastfield_accessor.num_vals() as u64 - 1);
        let slope = get_slope(first_val, last_val, fastfield_accessor.num_vals());
        // calculate offset to ensure all values are positive
        let mut offset = 0;
        let mut rel_positive_max = 0;
        for (pos, actual_value) in fastfield_accessor.iter().enumerate() {
            let calculated_value = get_calculated_value(first_val, pos as u64, slope);
            if calculated_value > actual_value {
                // negative value we need to apply an offset
                // we ignore negative values in the max value calculation, because negative values
                // will be offset to 0
                offset = offset.max(calculated_value - actual_value);
            } else {
                // positive value no offset reuqired
                rel_positive_max = rel_positive_max.max(actual_value - calculated_value);
            }
        }

        // rel_positive_max will be adjusted by offset
        let relative_max_value = rel_positive_max + offset;

        let num_bits = compute_num_bits(relative_max_value);
        let mut bit_packer = BitPacker::new();
        for (pos, val) in fastfield_accessor.iter().enumerate() {
            let calculated_value = get_calculated_value(first_val, pos as u64, slope);
            let diff = (val + offset) - calculated_value;
            bit_packer.write(diff, num_bits, write)?;
        }
        bit_packer.close(write)?;

        let footer = LinearFooter {
            relative_max_value,
            offset,
            first_val,
            last_val,
            num_vals: fastfield_accessor.num_vals(),
            min_value: fastfield_accessor.min_value(),
            max_value: fastfield_accessor.max_value(),
        };
        footer.serialize(write)?;
        Ok(())
    }
    fn is_applicable(fastfield_accessor: &impl FastFieldDataAccess) -> bool {
        if fastfield_accessor.num_vals() < 3 {
            return false; // disable compressor for this case
        }
        // On serialisation the offset is added to the actual value.
        // We need to make sure this won't run into overflow calculation issues.
        // For this we take the maximum theroretical offset and add this to the max value.
        // If this doesn't overflow the algorithm should be fine
        let theorethical_maximum_offset =
            fastfield_accessor.max_value() - fastfield_accessor.min_value();
        if fastfield_accessor
            .max_value()
            .checked_add(theorethical_maximum_offset)
            .is_none()
        {
            return false;
        }
        true
    }
    /// estimation for linear interpolation is hard because, you don't know
    /// where the local maxima for the deviation of the calculated value are and
    /// the offset to shift all values to >=0 is also unknown.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess) -> f32 {
        let first_val = fastfield_accessor.get_val(0);
        let last_val = fastfield_accessor.get_val(fastfield_accessor.num_vals() as u64 - 1);
        let slope = get_slope(first_val, last_val, fastfield_accessor.num_vals());

        // let's sample at 0%, 5%, 10% .. 95%, 100%
        let num_vals = fastfield_accessor.num_vals() as f32 / 100.0;
        let sample_positions = (0..20)
            .map(|pos| (num_vals * pos as f32 * 5.0) as usize)
            .collect::<Vec<_>>();

        let max_distance = sample_positions
            .iter()
            .map(|pos| {
                let calculated_value = get_calculated_value(first_val, *pos as u64, slope);
                let actual_value = fastfield_accessor.get_val(*pos as u64);
                distance(calculated_value, actual_value)
            })
            .max()
            .unwrap_or(0);

        // the theory would be that we don't have the actual max_distance, but we are close within
        // 50% threshold.
        // It is multiplied by 2 because in a log case scenario the line would be as much above as
        // below. So the offset would = max_distance
        //
        let relative_max_value = (max_distance as f32 * 1.5) * 2.0;

        let num_bits = compute_num_bits(relative_max_value as u64) as u64
            * fastfield_accessor.num_vals()
            + LinearFooter::SIZE_IN_BYTES as u64;
        let num_bits_uncompressed = 64 * fastfield_accessor.num_vals();
        num_bits as f32 / num_bits_uncompressed as f32
    }
}

#[inline]
fn distance<T: Sub<Output = T> + Ord>(x: T, y: T) -> T {
    if x < y {
        y - x
    } else {
        x - y
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::get_codec_test_data_sets;

    fn create_and_validate(data: &[u64], name: &str) -> (f32, f32) {
        crate::tests::create_and_validate::<LinearSerializer, LinearReader>(data, name)
    }

    #[test]
    fn get_calculated_value_test() {
        // pos slope
        assert_eq!(get_calculated_value(100, 10, 5.0), 150);

        // neg slope
        assert_eq!(get_calculated_value(100, 10, -5.0), 50);

        // pos slope, very high values
        assert_eq!(
            get_calculated_value(i64::MAX as u64, 10, 5.0),
            i64::MAX as u64 + 50
        );
        // neg slope, very high values
        assert_eq!(
            get_calculated_value(i64::MAX as u64, 10, -5.0),
            i64::MAX as u64 - 50
        );
    }

    #[test]
    fn test_compression() {
        let data = (10..=6_000_u64).collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate(&data, "simple monotonically large");

        assert!(actual_compression < 0.01);
        assert!(estimate < 0.01);
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
    fn linear_interpol_fast_field_test_large_amplitude() {
        let data = vec![
            i64::MAX as u64 / 2,
            i64::MAX as u64 / 3,
            i64::MAX as u64 / 2,
        ];

        create_and_validate(&data, "large amplitude");
    }
    #[test]
    fn linear_interpol_fast_concave_data() {
        let data = vec![0, 1, 2, 5, 8, 10, 20, 50];
        create_and_validate(&data, "concave data");
    }
    #[test]
    fn linear_interpol_fast_convex_data() {
        let data = vec![0, 40, 60, 70, 75, 77];
        create_and_validate(&data, "convex data");
    }
    #[test]
    fn linear_interpol_fast_field_test_simple() {
        let data = (10..=20_u64).collect::<Vec<_>>();

        create_and_validate(&data, "simple monotonically");
    }

    #[test]
    fn linear_interpol_fast_field_rand() {
        for _ in 0..5000 {
            let mut data = (0..50).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            create_and_validate(&data, "random");

            data.reverse();
            create_and_validate(&data, "random");
        }
    }
}
