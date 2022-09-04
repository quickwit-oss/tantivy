use std::io::{self, Write};

use common::{BinarySerializable, CountingWriter, DeserializeFrom};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::line::Line;
use crate::{Column, FastFieldCodec, FastFieldCodecType};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct LinearReader {
    data: OwnedBytes,
    footer: LinearParams,
}

impl Column for LinearReader {
    #[inline]
    fn get_val(&self, doc: u64) -> u64 {
        let interpoled_val: u64 = self.footer.line.eval(doc);
        let bitpacked_diff = self.footer.bit_unpacker.get(doc, &self.data);
        interpoled_val.wrapping_add(bitpacked_diff)
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
pub struct LinearCodec;

#[derive(Debug, Clone)]
struct LinearParams {
    num_vals: u64,
    min_value: u64,
    max_value: u64,
    line: Line,
    bit_unpacker: BitUnpacker,
}

impl BinarySerializable for LinearParams {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        self.num_vals.serialize(writer)?;
        self.min_value.serialize(writer)?;
        self.max_value.serialize(writer)?;
        self.line.serialize(writer)?;
        self.bit_unpacker.bit_width().serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let num_vals = u64::deserialize(reader)?;
        let min_value = u64::deserialize(reader)?;
        let max_value = u64::deserialize(reader)?;
        let line = Line::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        Ok(Self {
            num_vals,
            min_value,
            max_value,
            line,
            bit_unpacker: BitUnpacker::new(bit_width),
        })
    }
}

impl FastFieldCodec for LinearCodec {
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::Linear;

    type Reader = LinearReader;

    /// Opens a fast field given a file.
    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self::Reader> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;
        let (data, mut footer) = bytes.split(footer_offset);
        let footer = LinearParams::deserialize(&mut footer)?;
        Ok(LinearReader { data, footer })
    }

    /// Creates a new fast field serializer.
    fn serialize(write: &mut impl Write, fastfield_accessor: &dyn Column) -> io::Result<()> {
        assert!(fastfield_accessor.min_value() <= fastfield_accessor.max_value());
        let line = Line::train(fastfield_accessor);

        let max_offset_from_line = fastfield_accessor
            .iter()
            .enumerate()
            .map(|(pos, actual_value)| {
                let calculated_value = line.eval(pos as u64);
                let offset = actual_value.wrapping_sub(calculated_value);
                offset
            })
            .max()
            .unwrap();

        let num_bits = compute_num_bits(max_offset_from_line);
        let mut bit_packer = BitPacker::new();
        for (pos, actual_value) in fastfield_accessor.iter().enumerate() {
            let calculated_value = line.eval(pos as u64);
            let offset = actual_value.wrapping_sub(calculated_value);
            bit_packer.write(offset, num_bits, write)?;
        }
        bit_packer.close(write)?;

        let footer = LinearParams {
            num_vals: fastfield_accessor.num_vals(),
            min_value: fastfield_accessor.min_value(),
            max_value: fastfield_accessor.max_value(),
            line,
            bit_unpacker: BitUnpacker::new(num_bits),
        };

        let mut counting_wrt = CountingWriter::wrap(write);
        footer.serialize(&mut counting_wrt)?;
        let footer_len = counting_wrt.written_bytes();
        (footer_len as u32).serialize(&mut counting_wrt)?;

        Ok(())
    }

    /// estimation for linear interpolation is hard because, you don't know
    /// where the local maxima for the deviation of the calculated value are and
    /// the offset to shift all values to >=0 is also unknown.
    #[allow(clippy::question_mark)]
    fn estimate(fastfield_accessor: &impl Column) -> Option<f32> {
        if fastfield_accessor.num_vals() < 3 {
            return None; // disable compressor for this case
        }

        // let's sample at 0%, 5%, 10% .. 95%, 100%
        let num_vals = fastfield_accessor.num_vals() as f32 / 100.0;
        let sample_positions = (0..20)
            .map(|pos| (num_vals * pos as f32 * 5.0) as u64)
            .collect::<Vec<_>>();

        let line = Line::estimate(fastfield_accessor, &sample_positions);

        let estimated_bit_width = sample_positions
            .into_iter()
            .map(|pos| {
                let actual_value = fastfield_accessor.get_val(pos);
                let interpolated_val = line.eval(pos as u64);
                let diff = actual_value.wrapping_sub(interpolated_val);
                diff
            })
            .map(|diff| ((diff as f32 * 1.5) * 2.0) as u64)
            .map(compute_num_bits)
            .max()
            .unwrap_or(0);

        let num_bits = estimated_bit_width as u64 * fastfield_accessor.num_vals() as u64;
        let num_bits_uncompressed = 64 * fastfield_accessor.num_vals();
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

        assert!(actual_compression < 0.03);
        assert!(estimate < 0.04);
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
