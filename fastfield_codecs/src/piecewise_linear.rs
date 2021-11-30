/*!

PiecewiseLinear codec uses piecewise linear functions for every block of 512 values to guess values and stores the
difference between the actual value and the one given by the linear interpolation.
For every block, the linear function can be expressed as
`computed_value = slope * block_position + first_value + positive_offset`
where:
- `block_position` is the position inside of the block from 0 to 511
- `first_value` is the first value on the block
- `positive_offset` is computed such that we ensure the diff `real_value - computed_value` is always positive.

21 bytes is needed to store the block metadata, it adds an overhead of 21 * 8 / 512 = 0,33 bits per element.

*/

use crate::FastFieldCodecReader;
use crate::FastFieldCodecSerializer;
use crate::FastFieldDataAccess;
use crate::FastFieldStats;
use std::io::{self, Read, Write};
use std::ops::Sub;
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use common::BinarySerializable;
use common::DeserializeFrom;
use tantivy_bitpacker::BitUnpacker;

const BLOCK_SIZE: u64 = 512;

#[derive(Clone)]
pub struct PiecewiseLinearFastFieldReader {
    num_vals: u64,
    min_value: u64,
    max_value: u64,
    block_readers: Vec<BlockReader>,
}

/// Block metadata needed to define the linear function `y = a.x + b`
/// and to bitpack the difference between the real value and the
/// the linear function computed value where:
/// - `a` is the `slope`
/// - `b` is the sum of the `first_value` in the block + an offset
///   `positive_offset` which ensures that difference between the real
///   value and the linear function computed value is always positive.
#[derive(Clone, Debug, Default)]
struct BlockMetadata {
    first_value: u64,
    positive_offset: u64,
    slope: f32,
    num_bits: u8,
}

#[derive(Clone, Debug, Default)]
struct BlockReader {
    metadata: BlockMetadata,
    start_offset: u64,
    bit_unpacker: BitUnpacker,
}

impl BlockReader {
    fn new(metadata: BlockMetadata, start_offset: u64) -> Self {
        Self {
            bit_unpacker: BitUnpacker::new(metadata.num_bits),
            metadata,
            start_offset,
        }
    }

    #[inline]
    fn get_u64(&self, block_pos: u64, data: &[u8]) -> u64 {
        let diff = self
            .bit_unpacker
            .get(block_pos, &data[self.start_offset as usize..]);
        let computed_value =
            get_computed_value(self.metadata.first_value, block_pos, self.metadata.slope);
        (computed_value + diff) - self.metadata.positive_offset
    }
}

impl BinarySerializable for BlockMetadata {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.first_value.serialize(write)?;
        self.positive_offset.serialize(write)?;
        self.slope.serialize(write)?;
        self.num_bits.serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let constant = u64::deserialize(reader)?;
        let constant_positive_offset = u64::deserialize(reader)?;
        let slope = f32::deserialize(reader)?;
        let num_bits = u8::deserialize(reader)?;
        Ok(Self {
            first_value: constant,
            positive_offset: constant_positive_offset,
            slope,
            num_bits,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PiecewiseLinearFooter {
    pub num_vals: u64,
    pub min_value: u64,
    pub max_value: u64,
    block_metadatas: Vec<BlockMetadata>,
}

impl BinarySerializable for PiecewiseLinearFooter {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        let mut out = vec![];
        self.num_vals.serialize(&mut out)?;
        self.min_value.serialize(&mut out)?;
        self.max_value.serialize(&mut out)?;
        self.block_metadatas.serialize(&mut out)?;
        write.write_all(&out)?;
        (out.len() as u32).serialize(write)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let footer = Self {
            num_vals: u64::deserialize(reader)?,
            min_value: u64::deserialize(reader)?,
            max_value: u64::deserialize(reader)?,
            block_metadatas: Vec::<BlockMetadata>::deserialize(reader)?,
        };
        Ok(footer)
    }
}

impl FastFieldCodecReader for PiecewiseLinearFastFieldReader {
    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let (_, mut footer) = bytes.split_at(bytes.len() - (4 + footer_len) as usize);
        let footer = PiecewiseLinearFooter::deserialize(&mut footer)?;
        let mut block_readers = Vec::with_capacity(footer.block_metadatas.len());
        let mut current_data_offset = 0;
        for block_metadata in footer.block_metadatas.into_iter() {
            let num_bits = block_metadata.num_bits;
            block_readers.push(BlockReader::new(block_metadata, current_data_offset));
            current_data_offset += num_bits as u64 * BLOCK_SIZE / 8;
        }
        Ok(Self {
            num_vals: footer.num_vals,
            min_value: footer.min_value,
            max_value: footer.max_value,
            block_readers,
        })
    }

    #[inline]
    fn get_u64(&self, doc: u64, data: &[u8]) -> u64 {
        let block_idx = (doc / BLOCK_SIZE) as usize;
        let block_pos = doc - (block_idx as u64) * BLOCK_SIZE;
        let block_reader = &self.block_readers[block_idx];
        block_reader.get_u64(block_pos, data)
    }

    #[inline]
    fn min_value(&self) -> u64 {
        self.min_value
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.max_value
    }
}

#[inline]
fn get_computed_value(first_val: u64, pos: u64, slope: f32) -> u64 {
    (first_val as i64 + (pos as f32 * slope) as i64) as u64
}

pub struct PiecewiseLinearFastFieldSerializer;

impl FastFieldCodecSerializer for PiecewiseLinearFastFieldSerializer {
    const NAME: &'static str = "PiecewiseLinear";
    const ID: u8 = 5;
    /// Creates a new fast field serializer.
    fn serialize(
        write: &mut impl Write,
        _: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
        _data_iter1: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        let mut data = data_iter.collect::<Vec<_>>();
        let mut bit_packer = BitPacker::new();
        let mut block_metadatas = Vec::new();
        for data_pos in (0..data.len() as u64).step_by(BLOCK_SIZE as usize) {
            let block_num_vals = BLOCK_SIZE.min(data.len() as u64 - data_pos) as usize;
            let block_values = &mut data[data_pos as usize..data_pos as usize + block_num_vals];
            let slope = if block_num_vals == 1 {
                0f32
            } else {
                ((block_values[block_values.len() - 1] as f64 - block_values[0] as f64)
                    / (block_num_vals - 1) as f64) as f32
            };
            let first_value = block_values[0];
            let mut positive_offset = 0;
            let mut max_delta = 0;
            for (pos, &current_value) in block_values[1..].iter().enumerate() {
                let computed_value = get_computed_value(first_value, pos as u64 + 1, slope);
                if computed_value > current_value {
                    positive_offset = positive_offset.max(computed_value - current_value);
                } else {
                    max_delta = max_delta.max(current_value - computed_value);
                }
            }
            let num_bits = compute_num_bits(max_delta + positive_offset);
            for (pos, current_value) in block_values.iter().enumerate() {
                let computed_value = get_computed_value(first_value, pos as u64, slope);
                let diff = (current_value + positive_offset) - computed_value;
                bit_packer.write(diff, num_bits, write)?;
            }
            bit_packer.flush(write)?;
            block_metadatas.push(BlockMetadata {
                first_value,
                positive_offset,
                slope,
                num_bits,
            });
        }
        bit_packer.close(write)?;

        let footer = PiecewiseLinearFooter {
            num_vals: stats.num_vals,
            min_value: stats.min_value,
            max_value: stats.max_value,
            block_metadatas,
        };
        footer.serialize(write)?;
        Ok(())
    }

    fn is_applicable(
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> bool {
        if stats.num_vals < 10 * BLOCK_SIZE {
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

    /// Estimation for linear interpolation is hard because, you don't know
    /// where the local maxima are for the deviation of the calculated value and
    /// the offset is also unknown.
    fn estimate(fastfield_accessor: &impl FastFieldDataAccess, stats: FastFieldStats) -> f32 {
        let first_val_in_first_block = fastfield_accessor.get_val(0);
        let last_elem_in_first_chunk = BLOCK_SIZE.min(stats.num_vals);
        let last_val_in_first_block =
            fastfield_accessor.get_val(last_elem_in_first_chunk as u64 - 1);
        let slope = ((last_val_in_first_block as f64 - first_val_in_first_block as f64)
            / (stats.num_vals - 1) as f64) as f32;

        // let's sample at 0%, 5%, 10% .. 95%, 100%, but for the first block only
        let sample_positions = (0..20)
            .map(|pos| (last_elem_in_first_chunk as f32 / 100.0 * pos as f32 * 5.0) as usize)
            .collect::<Vec<_>>();

        let max_distance = sample_positions
            .iter()
            .map(|&pos| {
                let calculated_value =
                    get_computed_value(first_val_in_first_block, pos as u64, slope);
                let actual_value = fastfield_accessor.get_val(pos as u64);
                distance(calculated_value, actual_value)
            })
            .max()
            .unwrap();

        // Estimate one block and extrapolate the cost to all blocks.
        // the theory would be that we don't have the actual max_distance, but we are close within 50%
        // threshold.
        // It is multiplied by 2 because in a log case scenario the line would be as much above as
        // below. So the offset would = max_distance
        let relative_max_value = (max_distance as f32 * 1.5) * 2.0;

        let num_bits = compute_num_bits(relative_max_value as u64) as u64 * stats.num_vals as u64
            // function metadata per block
            + 21 * (stats.num_vals / BLOCK_SIZE);
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

    fn create_and_validate(data: &[u64], name: &str) -> (f32, f32) {
        crate::tests::create_and_validate::<
            PiecewiseLinearFastFieldSerializer,
            PiecewiseLinearFastFieldReader,
        >(data, name)
    }

    #[test]
    fn test_compression() {
        let data = (10..=6_000_u64).collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate(&data, "simple monotonically large");
        assert!(actual_compression < 0.2);
        assert!(estimate < 0.20);
        assert!(estimate > 0.15);
        assert!(actual_compression > 0.001);
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
            let (estimate, actual_compression) = create_and_validate(&data, "random");
            dbg!(estimate);
            dbg!(actual_compression);

            data.reverse();
            create_and_validate(&data, "random");
        }
    }
}
