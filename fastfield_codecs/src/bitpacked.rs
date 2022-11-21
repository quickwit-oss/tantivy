use std::io::{self, Write};

use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::serialize::NormalizedHeader;
use crate::{Column, FastFieldCodec, FastFieldCodecType};

/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    normalized_header: NormalizedHeader,
}

impl Column for BitpackedReader {
    #[inline]
    fn get_val(&self, doc: u32) -> u64 {
        self.bit_unpacker.get(doc, &self.data)
    }
    #[inline]
    fn min_value(&self) -> u64 {
        // The BitpackedReader assumes a normalized vector.
        0
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.normalized_header.max_value
    }
    #[inline]
    fn num_vals(&self) -> u32 {
        self.normalized_header.num_vals
    }
}

pub struct BitpackedCodec;

impl FastFieldCodec for BitpackedCodec {
    /// The CODEC_TYPE is an enum value used for serialization.
    const CODEC_TYPE: FastFieldCodecType = FastFieldCodecType::Bitpacked;

    type Reader = BitpackedReader;

    /// Opens a fast field given a file.
    fn open_from_bytes(
        data: OwnedBytes,
        normalized_header: NormalizedHeader,
    ) -> io::Result<Self::Reader> {
        let num_bits = compute_num_bits(normalized_header.max_value);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedReader {
            data,
            bit_unpacker,
            normalized_header,
        })
    }

    /// Serializes data with the BitpackedFastFieldSerializer.
    ///
    /// The bitpacker assumes that the column has been normalized.
    /// i.e. It has already been shifted by its minimum value, so that its
    /// current minimum value is 0.
    ///
    /// Ideally, we made a shift upstream on the column so that `col.min_value() == 0`.
    fn serialize(column: &dyn Column, write: &mut impl Write) -> io::Result<()> {
        assert_eq!(column.min_value(), 0u64);
        let num_bits = compute_num_bits(column.max_value());
        let mut bit_packer = BitPacker::new();
        for val in column.iter() {
            bit_packer.write(val, num_bits, write)?;
        }
        bit_packer.close(write)?;
        Ok(())
    }

    fn estimate(column: &dyn Column) -> Option<f32> {
        let num_bits = compute_num_bits(column.max_value());
        let num_bits_uncompressed = 64;
        Some(num_bits as f32 / num_bits_uncompressed as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::get_codec_test_datasets;

    fn create_and_validate(data: &[u64], name: &str) {
        crate::tests::create_and_validate::<BitpackedCodec>(data, name);
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate(&data, name);
            data.reverse();
            create_and_validate(&data, name);
        }
    }

    #[test]
    fn bitpacked_fast_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate(&data, "rand");

            data.reverse();
            create_and_validate(&data, "rand");
        }
    }
}
