use std::fmt::Debug;
use std::io;

use common::{BinarySerializable, VInt};

use crate::column_values::compact_space::CompactSpaceCompressor;
use crate::column_values::U128FastFieldCodecType;
use crate::iterable::Iterable;
use crate::MonotonicallyMappableToU128;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct U128Header {
    pub num_vals: u32,
    pub codec_type: U128FastFieldCodecType,
}

impl BinarySerializable for U128Header {
    fn serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.num_vals as u64).serialize(writer)?;
        self.codec_type.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let num_vals = VInt::deserialize(reader)?.0 as u32;
        let codec_type = U128FastFieldCodecType::deserialize(reader)?;
        Ok(U128Header {
            num_vals,
            codec_type,
        })
    }
}

/// Serializes u128 values with the compact space codec.
pub fn serialize_column_values_u128<T: MonotonicallyMappableToU128>(
    iterable: &dyn Iterable<T>,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let compressor = CompactSpaceCompressor::train_from(
        iterable
            .boxed_iter()
            .map(MonotonicallyMappableToU128::to_u128),
    );
    let header = U128Header {
        num_vals: compressor.num_vals(),
        codec_type: U128FastFieldCodecType::CompactSpace,
    };
    header.serialize(output)?;
    compressor.compress_into(
        iterable
            .boxed_iter()
            .map(MonotonicallyMappableToU128::to_u128),
        output,
    )?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::column_values::u64_based::{
        serialize_and_load_u64_based_column_values, serialize_u64_based_column_values,
        ALL_U64_CODEC_TYPES,
    };
    use crate::column_values::CodecType;

    #[test]
    fn test_serialize_deserialize_u128_header() {
        let original = U128Header {
            num_vals: 11,
            codec_type: U128FastFieldCodecType::CompactSpace,
        };
        let mut out = Vec::new();
        original.serialize(&mut out).unwrap();
        let restored = U128Header::deserialize(&mut &out[..]).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn test_serialize_deserialize() {
        let original = [1u64, 5u64, 10u64];
        let restored: Vec<u64> =
            serialize_and_load_u64_based_column_values(&&original[..], &ALL_U64_CODEC_TYPES)
                .iter()
                .collect();
        assert_eq!(&restored, &original[..]);
    }

    #[test]
    fn test_fastfield_bool_size_bitwidth_1() {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values::<bool>(
            &&[false, true][..],
            &ALL_U64_CODEC_TYPES,
            &mut buffer,
        )
        .unwrap();
        // TODO put the header as a footer so that it serves as a padding.
        // 5 bytes of header, 1 byte of value, 7 bytes of padding.
        assert_eq!(buffer.len(), 5 + 1);
    }

    #[test]
    fn test_fastfield_bool_bit_size_bitwidth_0() {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values::<bool>(
            &&[false, true][..],
            &ALL_U64_CODEC_TYPES,
            &mut buffer,
        )
        .unwrap();
        // 6 bytes of header, 0 bytes of value, 7 bytes of padding.
        assert_eq!(buffer.len(), 6);
    }

    #[test]
    fn test_fastfield_gcd() {
        let mut buffer = Vec::new();
        let vals: Vec<u64> = (0..80).map(|val| (val % 7) * 1_000u64).collect();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::Bitpacked], &mut buffer)
            .unwrap();
        // Values are stored over 3 bits.
        assert_eq!(buffer.len(), 6 + (3 * 80 / 8));
    }
}
