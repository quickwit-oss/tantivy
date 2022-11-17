// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

use common::{BinarySerializable, VInt};
use log::warn;
use ownedbytes::OwnedBytes;

use crate::bitpacked::BitpackedCodec;
use crate::blockwise_linear::BlockwiseLinearCodec;
use crate::compact_space::CompactSpaceCompressor;
use crate::linear::LinearCodec;
use crate::monotonic_mapping::{
    StrictlyMonotonicFn, StrictlyMonotonicMappingToInternal,
    StrictlyMonotonicMappingToInternalGCDBaseval,
};
use crate::{
    monotonic_map_column, Column, FastFieldCodec, FastFieldCodecType, MonotonicallyMappableToU64,
    U128FastFieldCodecType, VecColumn, ALL_CODEC_TYPES,
};

/// The normalized header gives some parameters after applying the following
/// normalization of the vector:
/// `val -> (val - min_value) / gcd`
///
/// By design, after normalization, `min_value = 0` and `gcd = 1`.
#[derive(Debug, Copy, Clone)]
pub struct NormalizedHeader {
    /// The number of values in the underlying column.
    pub num_vals: u32,
    /// The max value of the underlying column.
    pub max_value: u64,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Header {
    pub num_vals: u32,
    pub min_value: u64,
    pub max_value: u64,
    pub gcd: Option<NonZeroU64>,
    pub codec_type: FastFieldCodecType,
}

impl Header {
    pub fn normalized(self) -> NormalizedHeader {
        let gcd = self.gcd.map(|gcd| gcd.get()).unwrap_or(1);
        let gcd_min_val_mapping =
            StrictlyMonotonicMappingToInternalGCDBaseval::new(gcd, self.min_value);

        let max_value = gcd_min_val_mapping.mapping(self.max_value);
        NormalizedHeader {
            num_vals: self.num_vals,
            max_value,
        }
    }

    pub fn normalize_column<C: Column>(&self, from_column: C) -> impl Column {
        normalize_column(from_column, self.min_value, self.gcd)
    }

    pub fn compute_header(
        column: impl Column<u64>,
        codecs: &[FastFieldCodecType],
    ) -> Option<Header> {
        let num_vals = column.num_vals();
        let min_value = column.min_value();
        let max_value = column.max_value();
        let gcd = crate::gcd::find_gcd(column.iter().map(|val| val - min_value))
            .filter(|gcd| gcd.get() > 1u64);
        let normalized_column = normalize_column(column, min_value, gcd);
        let codec_type = detect_codec(normalized_column, codecs)?;
        Some(Header {
            num_vals,
            min_value,
            max_value,
            gcd,
            codec_type,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct U128Header {
    pub num_vals: u32,
    pub codec_type: U128FastFieldCodecType,
}

impl BinarySerializable for U128Header {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
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

pub fn normalize_column<C: Column>(
    from_column: C,
    min_value: u64,
    gcd: Option<NonZeroU64>,
) -> impl Column {
    let gcd = gcd.map(|gcd| gcd.get()).unwrap_or(1);
    let mapping = StrictlyMonotonicMappingToInternalGCDBaseval::new(gcd, min_value);
    monotonic_map_column(from_column, mapping)
}

impl BinarySerializable for Header {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.num_vals as u64).serialize(writer)?;
        VInt(self.min_value).serialize(writer)?;
        VInt(self.max_value - self.min_value).serialize(writer)?;
        if let Some(gcd) = self.gcd {
            VInt(gcd.get()).serialize(writer)?;
        } else {
            VInt(0u64).serialize(writer)?;
        }
        self.codec_type.serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let num_vals = VInt::deserialize(reader)?.0 as u32;
        let min_value = VInt::deserialize(reader)?.0;
        let amplitude = VInt::deserialize(reader)?.0;
        let max_value = min_value + amplitude;
        let gcd_u64 = VInt::deserialize(reader)?.0;
        let codec_type = FastFieldCodecType::deserialize(reader)?;
        Ok(Header {
            num_vals,
            min_value,
            max_value,
            gcd: NonZeroU64::new(gcd_u64),
            codec_type,
        })
    }
}

/// Return estimated compression for given codec in the value range [0.0..1.0], where 1.0 means no
/// compression.
pub fn estimate<T: MonotonicallyMappableToU64>(
    typed_column: impl Column<T>,
    codec_type: FastFieldCodecType,
) -> Option<f32> {
    let column = monotonic_map_column(typed_column, StrictlyMonotonicMappingToInternal::<T>::new());
    let min_value = column.min_value();
    let gcd = crate::gcd::find_gcd(column.iter().map(|val| val - min_value))
        .filter(|gcd| gcd.get() > 1u64);
    let mapping = StrictlyMonotonicMappingToInternalGCDBaseval::new(
        gcd.map(|gcd| gcd.get()).unwrap_or(1u64),
        min_value,
    );
    let normalized_column = monotonic_map_column(&column, mapping);
    match codec_type {
        FastFieldCodecType::Bitpacked => BitpackedCodec::estimate(&normalized_column),
        FastFieldCodecType::Linear => LinearCodec::estimate(&normalized_column),
        FastFieldCodecType::BlockwiseLinear => BlockwiseLinearCodec::estimate(&normalized_column),
    }
}

/// Serializes u128 values with the compact space codec.
pub fn serialize_u128<F: Fn() -> I, I: Iterator<Item = u128>>(
    iter_gen: F,
    num_vals: u32,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let header = U128Header {
        num_vals,
        codec_type: U128FastFieldCodecType::CompactSpace,
    };
    header.serialize(output)?;
    let compressor = CompactSpaceCompressor::train_from(iter_gen(), num_vals);
    compressor.compress_into(iter_gen(), output).unwrap();

    Ok(())
}

/// Serializes the column with the codec with the best estimate on the data.
pub fn serialize<T: MonotonicallyMappableToU64>(
    typed_column: impl Column<T>,
    output: &mut impl io::Write,
    codecs: &[FastFieldCodecType],
) -> io::Result<()> {
    let column = monotonic_map_column(typed_column, StrictlyMonotonicMappingToInternal::<T>::new());
    let header = Header::compute_header(&column, codecs).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Data cannot be serialized with this list of codec. {:?}",
                codecs
            ),
        )
    })?;
    header.serialize(output)?;
    let normalized_column = header.normalize_column(column);
    assert_eq!(normalized_column.min_value(), 0u64);
    serialize_given_codec(normalized_column, header.codec_type, output)?;
    Ok(())
}

fn detect_codec(
    column: impl Column<u64>,
    codecs: &[FastFieldCodecType],
) -> Option<FastFieldCodecType> {
    let mut estimations = Vec::new();
    for &codec in codecs {
        let estimation_opt = match codec {
            FastFieldCodecType::Bitpacked => BitpackedCodec::estimate(&column),
            FastFieldCodecType::Linear => LinearCodec::estimate(&column),
            FastFieldCodecType::BlockwiseLinear => BlockwiseLinearCodec::estimate(&column),
        };
        if let Some(estimation) = estimation_opt {
            estimations.push((estimation, codec));
        }
    }
    if let Some(broken_estimation) = estimations.iter().find(|estimation| estimation.0.is_nan()) {
        warn!(
            "broken estimation for fast field codec {:?}",
            broken_estimation.1
        );
    }
    // removing nan values for codecs with broken calculations, and max values which disables
    // codecs
    estimations.retain(|estimation| !estimation.0.is_nan() && estimation.0 != f32::MAX);
    estimations.sort_by(|(score_left, _), (score_right, _)| score_left.total_cmp(score_right));
    Some(estimations.first()?.1)
}

fn serialize_given_codec(
    column: impl Column<u64>,
    codec_type: FastFieldCodecType,
    output: &mut impl io::Write,
) -> io::Result<()> {
    match codec_type {
        FastFieldCodecType::Bitpacked => {
            BitpackedCodec::serialize(&column, output)?;
        }
        FastFieldCodecType::Linear => {
            LinearCodec::serialize(&column, output)?;
        }
        FastFieldCodecType::BlockwiseLinear => {
            BlockwiseLinearCodec::serialize(&column, output)?;
        }
    }
    output.flush()?;
    Ok(())
}

/// Helper function to serialize a column (autodetect from all codecs) and then open it
pub fn serialize_and_load<T: MonotonicallyMappableToU64 + Ord + Default>(
    column: &[T],
) -> Arc<dyn Column<T>> {
    let mut buffer = Vec::new();
    super::serialize(VecColumn::from(&column), &mut buffer, &ALL_CODEC_TYPES).unwrap();
    super::open(OwnedBytes::new(buffer))
        .unwrap()
        .to_full()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let restored: Vec<u64> = serialize_and_load(&original[..]).iter().collect();
        assert_eq!(&restored, &original[..]);
    }

    #[test]
    fn test_fastfield_bool_size_bitwidth_1() {
        let mut buffer = Vec::new();
        let col = VecColumn::from(&[false, true][..]);
        serialize(col, &mut buffer, &ALL_CODEC_TYPES).unwrap();
        // 5 bytes of header, 1 byte of value, 7 bytes of padding.
        assert_eq!(buffer.len(), 5 + 8);
    }

    #[test]
    fn test_fastfield_bool_bit_size_bitwidth_0() {
        let mut buffer = Vec::new();
        let col = VecColumn::from(&[true][..]);
        serialize(col, &mut buffer, &ALL_CODEC_TYPES).unwrap();
        // 5 bytes of header, 0 bytes of value, 7 bytes of padding.
        assert_eq!(buffer.len(), 5 + 7);
    }

    #[test]
    fn test_fastfield_gcd() {
        let mut buffer = Vec::new();
        let vals: Vec<u64> = (0..80).map(|val| (val % 7) * 1_000u64).collect();
        let col = VecColumn::from(&vals[..]);
        serialize(col, &mut buffer, &[FastFieldCodecType::Bitpacked]).unwrap();
        // Values are stored over 3 bits.
        assert_eq!(buffer.len(), 7 + (3 * 80 / 8) + 7);
    }
}
