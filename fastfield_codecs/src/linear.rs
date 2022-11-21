use std::io::{self, Write};

use common::BinarySerializable;
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::line::Line;
use crate::serialize::NormalizedHeader;
use crate::{Column, FastFieldCodec, FastFieldCodecType};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearReader {
    data: OwnedBytes,
    linear_params: LinearParams,
    header: NormalizedHeader,
}

impl Column for LinearReader {
    #[inline]
    fn get_val(&self, doc: u32) -> u64 {
        let interpoled_val: u64 = self.linear_params.line.eval(doc);
        let bitpacked_diff = self.linear_params.bit_unpacker.get(doc, &self.data);
        interpoled_val.wrapping_add(bitpacked_diff)
    }

    #[inline]
    fn min_value(&self) -> u64 {
        // The LinearReader assumes a normalized vector.
        0u64
    }

    #[inline]
    fn max_value(&self) -> u64 {
        self.header.max_value
    }

    #[inline]
    fn num_vals(&self) -> u32 {
        self.header.num_vals
    }
}

/// Fastfield serializer, which tries to guess values by linear interpolation
/// and stores the difference bitpacked.
pub struct LinearCodec;

#[derive(Debug, Clone)]
struct LinearParams {
    line: Line,
    bit_unpacker: BitUnpacker,
}

impl BinarySerializable for LinearParams {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        self.line.serialize(writer)?;
        self.bit_unpacker.bit_width().serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let line = Line::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        Ok(Self {
            line,
            bit_unpacker: BitUnpacker::new(bit_width),
        })
    }
}

impl FastFieldCodec for LinearCodec {
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::Linear;

    type Reader = LinearReader;

    /// Opens a fast field given a file.
    fn open_from_bytes(mut data: OwnedBytes, header: NormalizedHeader) -> io::Result<Self::Reader> {
        let linear_params = LinearParams::deserialize(&mut data)?;
        Ok(LinearReader {
            data,
            linear_params,
            header,
        })
    }

    /// Creates a new fast field serializer.
    fn serialize(column: &dyn Column, write: &mut impl Write) -> io::Result<()> {
        assert_eq!(column.min_value(), 0);
        let line = Line::train(column);

        let max_offset_from_line = column
            .iter()
            .enumerate()
            .map(|(pos, actual_value)| {
                let calculated_value = line.eval(pos as u32);
                actual_value.wrapping_sub(calculated_value)
            })
            .max()
            .unwrap();

        let num_bits = compute_num_bits(max_offset_from_line);
        let linear_params = LinearParams {
            line,
            bit_unpacker: BitUnpacker::new(num_bits),
        };
        linear_params.serialize(write)?;

        let mut bit_packer = BitPacker::new();
        for (pos, actual_value) in column.iter().enumerate() {
            let calculated_value = line.eval(pos as u32);
            let offset = actual_value.wrapping_sub(calculated_value);
            bit_packer.write(offset, num_bits, write)?;
        }
        bit_packer.close(write)?;

        Ok(())
    }

    /// estimation for linear interpolation is hard because, you don't know
    /// where the local maxima for the deviation of the calculated value are and
    /// the offset to shift all values to >=0 is also unknown.
    #[allow(clippy::question_mark)]
    fn estimate(column: &dyn Column) -> Option<f32> {
        if column.num_vals() < 3 {
            return None; // disable compressor for this case
        }

        let limit_num_vals = column.num_vals().min(100_000);

        let num_samples = 100;
        let step_size = (limit_num_vals / num_samples).max(1); // 20 samples
        let mut sample_positions_and_values: Vec<_> = Vec::new();
        for (pos, val) in column.iter().enumerate().step_by(step_size as usize) {
            sample_positions_and_values.push((pos as u64, val));
        }

        let line = Line::estimate(&sample_positions_and_values);

        let estimated_bit_width = sample_positions_and_values
            .into_iter()
            .map(|(pos, actual_value)| {
                let interpolated_val = line.eval(pos as u32);
                actual_value.wrapping_sub(interpolated_val)
            })
            .map(|diff| ((diff as f32 * 1.5) * 2.0) as u64)
            .map(compute_num_bits)
            .max()
            .unwrap_or(0);

        // Extrapolate to whole column
        let num_bits = (estimated_bit_width as u64 * column.num_vals() as u64) + 64;
        let num_bits_uncompressed = 64 * column.num_vals();
        Some(num_bits as f32 / num_bits_uncompressed as f32)
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use super::*;
    use crate::tests::get_codec_test_datasets;

    fn create_and_validate(data: &[u64], name: &str) -> Option<(f32, f32)> {
        crate::tests::create_and_validate::<LinearCodec>(data, name)
    }

    #[test]
    fn test_compression() {
        let data = (10..=6_000_u64).collect::<Vec<_>>();
        let (estimate, actual_compression) =
            create_and_validate(&data, "simple monotonically large").unwrap();

        assert_le!(actual_compression, 0.001);
        assert_le!(estimate, 0.02);
    }

    #[test]
    fn test_with_codec_datasets() {
        let data_sets = get_codec_test_datasets();
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
    fn overflow_error_test() {
        let data = vec![1572656989877777, 1170935903116329, 720575940379279, 0];
        create_and_validate(&data, "overflow test");
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
        let mut rng = rand::thread_rng();
        for _ in 0..50 {
            let mut data = (0..10_000).map(|_| rng.next_u64()).collect::<Vec<_>>();
            create_and_validate(&data, "random");
            data.reverse();
            create_and_validate(&data, "random");
        }
    }
}
