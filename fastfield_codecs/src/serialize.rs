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

use common::BinarySerializable;
use fastdivide::DividerU64;
use log::warn;
use ownedbytes::OwnedBytes;

use crate::bitpacked::BitpackedCodec;
use crate::blockwise_linear::BlockwiseLinearCodec;
use crate::gcd::{find_gcd, GCDParams};
use crate::linear::LinearCodec;
use crate::{
    monotonic_map_column, Column, FastFieldCodec, FastFieldCodecType, MonotonicallyMappableToU64,
    VecColumn, ALL_CODEC_TYPES,
};

// use this, when this is merged and stabilized explicit_generic_args_with_impl_trait
// https://github.com/rust-lang/rust/pull/86176
fn codec_estimation<C: FastFieldCodec, D: Column>(
    fastfield_accessor: &D,
    estimations: &mut Vec<(f32, FastFieldCodecType)>,
) {
    if let Some(ratio) = C::estimate(fastfield_accessor) {
        estimations.push((ratio, C::CODEC_TYPE));
    }
}

fn write_header<W: io::Write>(codec_type: FastFieldCodecType, output: &mut W) -> io::Result<()> {
    codec_type.to_code().serialize(output)?;
    Ok(())
}

fn gcd_params(column: &impl Column<u64>) -> Option<GCDParams> {
    let min_value = column.min_value();
    let gcd = find_gcd(column.iter().map(|val| val - min_value)).map(NonZeroU64::get)?;
    if gcd == 1 {
        return None;
    }
    Some(GCDParams {
        gcd,
        min_value,
        num_vals: column.num_vals(),
    })
}

/// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
pub fn open<T: MonotonicallyMappableToU64>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn Column<T>>> {
    let codec_type = FastFieldCodecType::deserialize(&mut bytes)?;
    open_from_id(bytes, codec_type)
}

fn open_codec_from_bytes<C: FastFieldCodec, Item: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn Column<Item>>> {
    let reader = C::open_from_bytes(bytes)?;
    Ok(Arc::new(monotonic_map_column(reader, Item::from_u64)))
}

pub fn open_gcd_from_bytes<WrappedCodec: FastFieldCodec>(
    bytes: OwnedBytes,
) -> io::Result<impl Column> {
    let footer_offset = bytes.len() - 24;
    let (body, mut footer) = bytes.split(footer_offset);
    let gcd_params = GCDParams::deserialize(&mut footer)?;
    let gcd_remap = move |val: u64| gcd_params.min_value + gcd_params.gcd * val;
    let reader: WrappedCodec::Reader = WrappedCodec::open_from_bytes(body)?;
    Ok(monotonic_map_column(reader, gcd_remap))
}

fn open_codec_with_gcd<C: FastFieldCodec, Item: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn Column<Item>>> {
    let reader = open_gcd_from_bytes::<C>(bytes)?;
    Ok(Arc::new(monotonic_map_column(reader, Item::from_u64)))
}

/// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
fn open_from_id<T: MonotonicallyMappableToU64>(
    mut bytes: OwnedBytes,
    codec_type: FastFieldCodecType,
) -> io::Result<Arc<dyn Column<T>>> {
    match codec_type {
        FastFieldCodecType::Bitpacked => open_codec_from_bytes::<BitpackedCodec, _>(bytes),
        FastFieldCodecType::Linear => open_codec_from_bytes::<LinearCodec, _>(bytes),
        FastFieldCodecType::BlockwiseLinear => {
            open_codec_from_bytes::<BlockwiseLinearCodec, _>(bytes)
        }
        FastFieldCodecType::Gcd => {
            let codec_type = FastFieldCodecType::deserialize(&mut bytes)?;
            match codec_type {
                FastFieldCodecType::Bitpacked => open_codec_with_gcd::<BitpackedCodec, _>(bytes),
                FastFieldCodecType::Linear => open_codec_with_gcd::<LinearCodec, _>(bytes),
                FastFieldCodecType::BlockwiseLinear => {
                    open_codec_with_gcd::<BlockwiseLinearCodec, _>(bytes)
                }
                FastFieldCodecType::Gcd => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Gcd codec wrapped into another gcd codec. This combination is not allowed.",
                )),
            }
        }
    }
}

pub fn serialize<T: MonotonicallyMappableToU64>(
    typed_column: impl Column<T>,
    output: &mut impl io::Write,
    codecs: &[FastFieldCodecType],
) -> io::Result<()> {
    let column = monotonic_map_column(typed_column, T::to_u64);
    let gcd_params_opt = if codecs.contains(&FastFieldCodecType::Gcd) {
        gcd_params(&column)
    } else {
        None
    };

    let gcd_params = if let Some(gcd_params) = gcd_params_opt {
        gcd_params
    } else {
        return serialize_without_gcd(column, output, codecs);
    };

    write_header(FastFieldCodecType::Gcd, output)?;
    let base_value = column.min_value();
    let gcd_divider = DividerU64::divide_by(gcd_params.gcd);
    let divided_fastfield_accessor =
        monotonic_map_column(column, |val: u64| gcd_divider.divide(val - base_value));

    serialize_without_gcd(divided_fastfield_accessor, output, codecs)?;

    gcd_params.serialize(output)?;
    Ok(())
}

fn serialize_without_gcd(
    column: impl Column<u64>,
    output: &mut impl io::Write,
    codecs: &[FastFieldCodecType],
) -> io::Result<()> {
    let mut estimations = Vec::new();
    for &codec in codecs {
        if codec == FastFieldCodecType::Gcd {
            continue;
        }
        match codec {
            FastFieldCodecType::Bitpacked => {
                codec_estimation::<BitpackedCodec, _>(&column, &mut estimations);
            }
            FastFieldCodecType::Linear => {
                codec_estimation::<LinearCodec, _>(&column, &mut estimations);
            }
            FastFieldCodecType::BlockwiseLinear => {
                codec_estimation::<BlockwiseLinearCodec, _>(&column, &mut estimations);
            }
            FastFieldCodecType::Gcd => {}
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
    estimations.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let (_ratio, codec_type) = estimations[0];

    write_header(codec_type, output)?;
    match codec_type {
        FastFieldCodecType::Bitpacked => {
            BitpackedCodec::serialize(output, &column)?;
        }
        FastFieldCodecType::Linear => {
            LinearCodec::serialize(output, &column)?;
        }
        FastFieldCodecType::BlockwiseLinear => {
            BlockwiseLinearCodec::serialize(output, &column)?;
        }
        FastFieldCodecType::Gcd => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GCD codec not supported.",
            ));
        }
    }
    output.flush()?;
    Ok(())
}

pub fn serialize_and_load<T: MonotonicallyMappableToU64 + Ord + Default>(
    column: &[T],
) -> Arc<dyn Column<T>> {
    let mut buffer = Vec::new();
    super::serialize(VecColumn::from(column), &mut buffer, &ALL_CODEC_TYPES).unwrap();
    super::open(OwnedBytes::new(buffer)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let original = [1u64, 5u64, 10u64];
        let restored: Vec<u64> = serialize_and_load(&original[..]).iter().collect();
        assert_eq!(&restored, &original[..]);
    }
}
