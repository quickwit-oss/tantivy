/*!

MultiLinearInterpol compressor uses linear interpolation to guess a values and stores the offset, but in blocks of 512.

With a CHUNK_SIZE of 512 and 29 byte metadata per block, we get a overhead for metadata of 232 / 512 = 0,45 bits per element.
The additional space required per element in a block is the the maximum deviation of the linear interpolation estimation function.

E.g. if the maximum deviation of an element is 12, all elements cost 4bits.

Size per block:
Num Elements * Maximum Deviation from Interpolation + 29 Byte Metadata

*/

use crate::FastFieldCodecReader;
use crate::FastFieldCodecSerializer;
use crate::FastFieldDataAccess;
use crate::FastFieldStats;
use common::CountingWriter;
use std::io::{self, Read, Write};
use std::ops::Sub;
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use common::BinarySerializable;
use common::DeserializeFrom;
use tantivy_bitpacker::BitUnpacker;

const CHUNK_SIZE: u64 = 512;

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct MultiLinearInterpolFastFieldReader {
    pub footer: MultiLinearInterpolFooter,
}

#[derive(Clone, Debug, Default)]
struct Function {
    // The offset in the data is required, because we have diffrent bit_widths per block
    data_start_offset: u64,
    // start_pos in the block will be CHUNK_SIZE * BLOCK_NUM
    start_pos: u64,
    // only used during serialization, 0 after deserialization
    end_pos: u64,
    // only used during serialization, 0 after deserialization
    value_start_pos: u64,
    // only used during serialization, 0 after deserialization
    value_end_pos: u64,
    slope: f32,
    // The offset so that all values are positive when writing them
    positive_val_offset: u64,
    num_bits: u8,
    bit_unpacker: BitUnpacker,
}

impl Function {
    fn calc_slope(&mut self) {
        let num_vals = self.end_pos - self.start_pos;
        get_slope(self.value_start_pos, self.value_end_pos, num_vals);
    }
    // split the interpolation into two function, change self and return the second split
    fn split(&mut self, split_pos: u64, split_pos_value: u64) -> Function {
        let mut new_function = Function {
            start_pos: split_pos,
            end_pos: self.end_pos,
            value_start_pos: split_pos_value,
            value_end_pos: self.value_end_pos,
            ..Default::default()
        };
        new_function.calc_slope();
        self.end_pos = split_pos;
        self.value_end_pos = split_pos_value;
        self.calc_slope();
        new_function
    }
}

impl BinarySerializable for Function {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.data_start_offset.serialize(write)?;
        self.value_start_pos.serialize(write)?;
        self.positive_val_offset.serialize(write)?;
        self.slope.serialize(write)?;
        self.num_bits.serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Function> {
        let data_start_offset = u64::deserialize(reader)?;
        let value_start_pos = u64::deserialize(reader)?;
        let offset = u64::deserialize(reader)?;
        let slope = f32::deserialize(reader)?;
        let num_bits = u8::deserialize(reader)?;
        let interpolation = Function {
            data_start_offset,
            value_start_pos,
            positive_val_offset: offset,
            num_bits,
            bit_unpacker: BitUnpacker::new(num_bits),
            slope,
            ..Default::default()
        };

        Ok(interpolation)
    }
}

#[derive(Clone, Debug)]
pub struct MultiLinearInterpolFooter {
    pub num_vals: u64,
    pub min_value: u64,
    pub max_value: u64,
    interpolations: Vec<Function>,
}

impl BinarySerializable for MultiLinearInterpolFooter {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        let mut out = vec![];
        self.num_vals.serialize(&mut out)?;
        self.min_value.serialize(&mut out)?;
        self.max_value.serialize(&mut out)?;
        self.interpolations.serialize(&mut out)?;
        write.write_all(&out)?;
        (out.len() as u32).serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<MultiLinearInterpolFooter> {
        let mut footer = MultiLinearInterpolFooter {
            num_vals: u64::deserialize(reader)?,
            min_value: u64::deserialize(reader)?,
            max_value: u64::deserialize(reader)?,
            interpolations: Vec::<Function>::deserialize(reader)?,
        };
        for (num, interpol) in footer.interpolations.iter_mut().enumerate() {
            interpol.start_pos = CHUNK_SIZE * num as u64;
        }
        Ok(footer)
    }
}

#[inline]
fn get_interpolation_position(doc: u64) -> usize {
    let index = doc / CHUNK_SIZE;
    index as usize
}

#[inline]
fn get_interpolation_function(doc: u64, interpolations: &[Function]) -> &Function {
    &interpolations[get_interpolation_position(doc)]
}

impl FastFieldCodecReader for MultiLinearInterpolFastFieldReader {
    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;

        let (_data, mut footer) = bytes.split_at(bytes.len() - (4 + footer_len) as usize);
        let footer = MultiLinearInterpolFooter::deserialize(&mut footer)?;

        Ok(MultiLinearInterpolFastFieldReader { footer })
    }

    #[inline]
    fn get_u64(&self, doc: u64, data: &[u8]) -> u64 {
        let interpolation = get_interpolation_function(doc, &self.footer.interpolations);
        let doc = doc - interpolation.start_pos;
        let calculated_value =
            get_calculated_value(interpolation.value_start_pos, doc, interpolation.slope);
        let diff = interpolation
            .bit_unpacker
            .get(doc, &data[interpolation.data_start_offset as usize..]);
        (calculated_value + diff) - interpolation.positive_val_offset
    }

    #[inline]
    fn min_value(&self) -> u64 {
        self.footer.min_value
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.footer.max_value
    }
}

#[inline]
fn get_slope(first_val: u64, last_val: u64, num_vals: u64) -> f32 {
    ((last_val as f64 - first_val as f64) / (num_vals as u64 - 1) as f64) as f32
}

#[inline]
fn get_calculated_value(first_val: u64, pos: u64, slope: f32) -> u64 {
    (first_val as i64 + (pos as f32 * slope) as i64) as u64
}

/// Same as LinearInterpolFastFieldSerializer, but working on chunks of CHUNK_SIZE elements.
pub struct MultiLinearInterpolFastFieldSerializer {}

impl FastFieldCodecSerializer for MultiLinearInterpolFastFieldSerializer {
    const NAME: &'static str = "MultiLinearInterpol";
    const ID: u8 = 3;
    /// Creates a new fast field serializer.
    fn serialize(
        write: &mut impl Write,
        fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        _data_iter1: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        assert!(stats.min_value <= stats.max_value);

        let first_val = fastfield_accessor.get_val(0);
        let last_val = fastfield_accessor.get_val(stats.num_vals as u64 - 1);

        let mut first_function = Function {
            end_pos: stats.num_vals,
            value_start_pos: first_val,
            value_end_pos: last_val,
            ..Default::default()
        };
        first_function.calc_slope();
        let mut interpolations = vec![first_function];

        // Since we potentially apply multiple passes over the data, the data is cached.
        // Multiple iteration can be expensive (merge with index sorting can add lot of overhead per
        // iteration)
        let data = data_iter.collect::<Vec<_>>();

        //// let's split this into chunks of CHUNK_SIZE
        for data_pos in (0..data.len() as u64).step_by(CHUNK_SIZE as usize).skip(1) {
            let new_fun = {
                let current_interpolation = interpolations.last_mut().unwrap();
                current_interpolation.split(data_pos, data[data_pos as usize])
            };
            interpolations.push(new_fun);
        }
        // calculate offset and max (-> numbits) for each function
        for interpolation in &mut interpolations {
            let mut offset = 0;
            let mut rel_positive_max = 0;
            for (pos, actual_value) in data
                [interpolation.start_pos as usize..interpolation.end_pos as usize]
                .iter()
                .cloned()
                .enumerate()
            {
                let calculated_value = get_calculated_value(
                    interpolation.value_start_pos,
                    pos as u64,
                    interpolation.slope,
                );
                if calculated_value > actual_value {
                    // negative value we need to apply an offset
                    // we ignore negative values in the max value calculation, because negative values
                    // will be offset to 0
                    offset = offset.max(calculated_value - actual_value);
                } else {
                    //positive value no offset reuqired
                    rel_positive_max = rel_positive_max.max(actual_value - calculated_value);
                }
            }

            interpolation.positive_val_offset = offset;
            interpolation.num_bits = compute_num_bits(rel_positive_max + offset);
        }
        let mut bit_packer = BitPacker::new();

        let write = &mut CountingWriter::wrap(write);
        for interpolation in &mut interpolations {
            interpolation.data_start_offset = write.written_bytes();
            let num_bits = interpolation.num_bits;
            for (pos, actual_value) in data
                [interpolation.start_pos as usize..interpolation.end_pos as usize]
                .iter()
                .cloned()
                .enumerate()
            {
                let calculated_value = get_calculated_value(
                    interpolation.value_start_pos,
                    pos as u64,
                    interpolation.slope,
                );
                let diff = (actual_value + interpolation.positive_val_offset) - calculated_value;
                bit_packer.write(diff, num_bits, write)?;
            }
            bit_packer.flush(write)?;
        }
        bit_packer.close(write)?;

        let footer = MultiLinearInterpolFooter {
            num_vals: stats.num_vals,
            min_value: stats.min_value,
            max_value: stats.max_value,
            interpolations,
        };
        footer.serialize(write)?;
        Ok(())
    }

    fn is_applicable(
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> bool {
        if stats.num_vals < 5_000 {
            return false;
        }
        // On serialization the offset is added to the actual value.
        // We need to make sure this won't run into overflow calculation issues.
        // For this we take the maximum theroretical offset and add this to the max value.
        // If this doesn't overflow the algortihm should be fine
        let theorethical_maximum_offset = stats.max_value - stats.min_value;
        if stats
            .max_value
            .checked_add(theorethical_maximum_offset)
            .is_none()
        {
            return false;
        }
        true
    }
    /// estimation for linear interpolation is hard because, you don't know
    /// where the local maxima are for the deviation of the calculated value and
    /// the offset is also unknown.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> f32 {
        let first_val_in_first_block = fastfield_accessor.get_val(0);
        let last_elem_in_first_chunk = CHUNK_SIZE.min(stats.num_vals);
        let last_val_in_first_block =
            fastfield_accessor.get_val(last_elem_in_first_chunk as u64 - 1);
        let slope = get_slope(
            first_val_in_first_block,
            last_val_in_first_block,
            stats.num_vals,
        );

        // let's sample at 0%, 5%, 10% .. 95%, 100%, but for the first block only
        let sample_positions = (0..20)
            .map(|pos| (last_elem_in_first_chunk as f32 / 100.0 * pos as f32 * 5.0) as usize)
            .collect::<Vec<_>>();

        let max_distance = sample_positions
            .iter()
            .map(|pos| {
                let calculated_value =
                    get_calculated_value(first_val_in_first_block, *pos as u64, slope);
                let actual_value = fastfield_accessor.get_val(*pos as u64);
                distance(calculated_value, actual_value)
            })
            .max()
            .unwrap();

        // Estimate one block and extrapolate the cost to all blocks.
        // the theory would be that we don't have the actual max_distance, but we are close within 50%
        // threshold.
        // It is multiplied by 2 because in a log case scenario the line would be as much above as
        // below. So the offset would = max_distance
        //
        let relative_max_value = (max_distance as f32 * 1.5) * 2.0;

        let num_bits = compute_num_bits(relative_max_value as u64) as u64 * stats.num_vals as u64
            // function metadata per block
            + 29 * (stats.num_vals / CHUNK_SIZE);
        let num_bits_uncompressed = 64 * stats.num_vals;
        num_bits as f32 / num_bits_uncompressed as f32
    }
}

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

    fn create_and_validate(data: &[u64], name: &str) {
        crate::tests::create_and_validate::<
            MultiLinearInterpolFastFieldSerializer,
            MultiLinearInterpolFastFieldReader,
        >(data, name);
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
    fn test_simple() {
        let data = (10..=20_u64).collect::<Vec<_>>();
        create_and_validate(&data, "simple monotonically");
    }

    #[test]
    fn border_cases_1() {
        let data = (0..1024).collect::<Vec<_>>();
        create_and_validate(&data, "border case");
    }
    #[test]
    fn border_case_2() {
        let data = (0..1025).collect::<Vec<_>>();
        create_and_validate(&data, "border case");
    }
    #[test]
    fn rand() {
        for _ in 0..10 {
            let mut data = (5_000..20_000)
                .map(|_| rand::random::<u64>() as u64)
                .collect::<Vec<_>>();
            create_and_validate(&data, "random");

            data.reverse();
            create_and_validate(&data, "random");
        }
    }
}
