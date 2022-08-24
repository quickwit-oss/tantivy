//! The BlockwiseLinear codec uses linear interpolation to guess a values and stores the
//! offset, but in blocks of 512.
//!
//! With a CHUNK_SIZE of 512 and 29 byte metadata per block, we get a overhead for metadata of 232 /
//! 512 = 0,45 bits per element. The additional space required per element in a block is the the
//! maximum deviation of the linear interpolation estimation function.
//!
//! E.g. if the maximum deviation of an element is 12, all elements cost 4bits.
//!
//! Size per block:
//! Num Elements * Maximum Deviation from Interpolation + 29 Byte Metadata

use std::io::{self, Read, Write};
use std::ops::Sub;

use common::{BinarySerializable, CountingWriter, DeserializeFrom};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::linear::{get_calculated_value, get_slope};
use crate::{
    FastFieldCodecReader, FastFieldCodecSerializer, FastFieldCodecType, FastFieldDataAccess,
};

const CHUNK_SIZE: u64 = 512;

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BlockwiseLinearReader {
    data: OwnedBytes,
    pub footer: BlockwiseLinearFooter,
}

#[derive(Clone, Debug, Default)]
struct Function {
    // The offset in the data is required, because we have different bit_widths per block
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
        self.slope = get_slope(self.value_start_pos, self.value_end_pos, num_vals);
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
pub struct BlockwiseLinearFooter {
    pub num_vals: u64,
    pub min_value: u64,
    pub max_value: u64,
    interpolations: Vec<Function>,
}

impl BinarySerializable for BlockwiseLinearFooter {
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

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<BlockwiseLinearFooter> {
        let mut footer = BlockwiseLinearFooter {
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

impl FastFieldCodecReader for BlockwiseLinearReader {
    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;
        let (data, mut footer) = bytes.split(footer_offset);
        let footer = BlockwiseLinearFooter::deserialize(&mut footer)?;
        Ok(BlockwiseLinearReader { data, footer })
    }

    #[inline]
    fn get_u64(&self, idx: u64) -> u64 {
        let interpolation = get_interpolation_function(idx, &self.footer.interpolations);
        let in_block_idx = idx - interpolation.start_pos;
        let calculated_value = get_calculated_value(
            interpolation.value_start_pos,
            in_block_idx,
            interpolation.slope,
        );
        let diff = interpolation.bit_unpacker.get(
            in_block_idx,
            &self.data[interpolation.data_start_offset as usize..],
        );
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
    #[inline]
    fn num_vals(&self) -> u64 {
        self.footer.num_vals
    }
}

/// Same as LinearSerializer, but working on chunks of CHUNK_SIZE elements.
pub struct BlockwiseLinearSerializer {}

impl FastFieldCodecSerializer for BlockwiseLinearSerializer {
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::BlockwiseLinear;
    /// Creates a new fast field serializer.
    fn serialize(
        write: &mut impl Write,
        fastfield_accessor: &dyn FastFieldDataAccess,
    ) -> io::Result<()> {
        assert!(fastfield_accessor.min_value() <= fastfield_accessor.max_value());

        let first_val = fastfield_accessor.get_val(0);
        let last_val = fastfield_accessor.get_val(fastfield_accessor.num_vals() as u64 - 1);

        let mut first_function = Function {
            end_pos: fastfield_accessor.num_vals(),
            value_start_pos: first_val,
            value_end_pos: last_val,
            ..Default::default()
        };
        first_function.calc_slope();
        let mut interpolations = vec![first_function];

        // Since we potentially apply multiple passes over the data, the data is cached.
        // Multiple iteration can be expensive (merge with index sorting can add lot of overhead per
        // iteration)
        let data = fastfield_accessor.iter().collect::<Vec<_>>();

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
                    // we ignore negative values in the max value calculation, because negative
                    // values will be offset to 0
                    offset = offset.max(calculated_value - actual_value);
                } else {
                    // positive value no offset reuqired
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

        let footer = BlockwiseLinearFooter {
            num_vals: fastfield_accessor.num_vals(),
            min_value: fastfield_accessor.min_value(),
            max_value: fastfield_accessor.max_value(),
            interpolations,
        };
        footer.serialize(write)?;
        Ok(())
    }

    fn is_applicable(fastfield_accessor: &impl FastFieldDataAccess) -> bool {
        if fastfield_accessor.num_vals() < 5_000 {
            return false;
        }
        // On serialization the offset is added to the actual value.
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
    /// where the local maxima are for the deviation of the calculated value and
    /// the offset is also unknown.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess) -> f32 {
        let first_val_in_first_block = fastfield_accessor.get_val(0);
        let last_elem_in_first_chunk = CHUNK_SIZE.min(fastfield_accessor.num_vals());
        let last_val_in_first_block =
            fastfield_accessor.get_val(last_elem_in_first_chunk as u64 - 1);
        let slope = get_slope(
            first_val_in_first_block,
            last_val_in_first_block,
            fastfield_accessor.num_vals(),
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
        // the theory would be that we don't have the actual max_distance, but we are close within
        // 50% threshold.
        // It is multiplied by 2 because in a log case scenario the line would be as much above as
        // below. So the offset would = max_distance
        //
        let relative_max_value = (max_distance as f32 * 1.5) * 2.0;

        let num_bits = compute_num_bits(relative_max_value as u64) as u64 * fastfield_accessor.num_vals() as u64
            // function metadata per block
            + 29 * (fastfield_accessor.num_vals() / CHUNK_SIZE);
        let num_bits_uncompressed = 64 * fastfield_accessor.num_vals();
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

    fn create_and_validate(data: &[u64], name: &str) -> (f32, f32) {
        crate::tests::create_and_validate::<BlockwiseLinearSerializer, BlockwiseLinearReader>(
            data, name,
        )
    }

    const HIGHEST_BIT: u64 = 1 << 63;
    pub fn i64_to_u64(val: i64) -> u64 {
        (val as u64) ^ HIGHEST_BIT
    }

    #[test]
    fn test_compression_i64() {
        let data = (i64::MAX - 600_000..=i64::MAX - 550_000)
            .map(i64_to_u64)
            .collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate(&data, "simple monotonically large i64");
        assert!(actual_compression < 0.2);
        assert!(estimate < 0.20);
        assert!(estimate > 0.15);
        assert!(actual_compression > 0.01);
    }

    #[test]
    fn test_compression() {
        let data = (10..=6_000_u64).collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate(&data, "simple monotonically large");
        assert!(actual_compression < 0.2);
        assert!(estimate < 0.20);
        assert!(estimate > 0.15);
        assert!(actual_compression > 0.01);
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
                .map(|_| rand::random::<u32>() as u64)
                .collect::<Vec<_>>();
            let _ = create_and_validate(&data, "random");
            data.reverse();
            create_and_validate(&data, "random");
        }
    }
}
