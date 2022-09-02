use std::sync::Arc;

use common::BinarySerializable;
use fastfield_codecs::bitpacked::BitpackedCodec;
use fastfield_codecs::blockwise_linear::BlockwiseLinearCodec;
use fastfield_codecs::linear::LinearCodec;
use fastfield_codecs::{monotonic_map_column, Column, FastFieldCodec, FastFieldCodecType};

use super::gcd::open_gcd_from_bytes;
use super::FastValue;
use crate::directory::OwnedBytes;
use crate::error::DataCorruption;

fn open_codec_from_bytes<C: FastFieldCodec, Item: FastValue>(
    bytes: OwnedBytes,
) -> crate::Result<Arc<dyn Column<Item>>> {
    let reader = C::open_from_bytes(bytes)?;
    Ok(Arc::new(monotonic_map_column(reader, Item::from_u64)))
}

fn open_codec_with_gcd<C: FastFieldCodec, Item: FastValue>(
    bytes: OwnedBytes,
) -> crate::Result<Arc<dyn Column<Item>>> {
    let reader = open_gcd_from_bytes::<C>(bytes)?;
    Ok(Arc::new(monotonic_map_column(reader, Item::from_u64)))
}

/// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
fn open_from_id<Item: FastValue>(
    mut bytes: OwnedBytes,
    codec_type: FastFieldCodecType,
) -> crate::Result<Arc<dyn Column<Item>>> {
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
                FastFieldCodecType::Gcd => Err(DataCorruption::comment_only(
                    "Gcd codec wrapped into another gcd codec. This combination is not allowed.",
                )
                .into()),
            }
        }
    }
}

/// Returns correct the reader wrapped in the `DynamicFastFieldReader` enum for the data.
pub fn open_fast_field<Item: FastValue>(
    mut bytes: OwnedBytes,
) -> crate::Result<Arc<dyn Column<Item>>> {
    let codec_type = FastFieldCodecType::deserialize(&mut bytes)?;
    open_from_id(bytes, codec_type)
}
