use std::marker::PhantomData;
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

/// Wrapper for accessing a fastfield.
///
/// Holds the data and the codec to the read the data.
#[derive(Clone)]
pub struct FastFieldReaderCodecWrapper<Item: FastValue, CodecReader> {
    reader: CodecReader,
    _phantom: PhantomData<Item>,
}

impl<Item: FastValue, CodecReader> From<CodecReader>
    for FastFieldReaderCodecWrapper<Item, CodecReader>
{
    fn from(reader: CodecReader) -> Self {
        FastFieldReaderCodecWrapper {
            reader,
            _phantom: PhantomData,
        }
    }
}

impl<Item: FastValue, D: Column> FastFieldReaderCodecWrapper<Item, D> {
    #[inline]
    pub(crate) fn get_u64(&self, idx: u64) -> Item {
        let data = self.reader.get_val(idx);
        Item::from_u64(data)
    }

    /// Internally `multivalued` also use SingleValue Fast fields.
    /// It works as follows... A first column contains the list of start index
    /// for each document, a second column contains the actual values.
    ///
    /// The values associated to a given doc, are then
    ///  `second_column[first_column.get(doc)..first_column.get(doc+1)]`.
    ///
    /// Which means single value fast field reader can be indexed internally with
    /// something different from a `DocId`. For this use case, we want to use `u64`
    /// values.
    ///
    /// See `get_range` for an actual documentation about this method.
    pub(crate) fn get_range_u64(&self, start: u64, output: &mut [Item]) {
        for (i, out) in output.iter_mut().enumerate() {
            *out = self.get_u64(start + (i as u64));
        }
    }
}

impl<Item: FastValue, C: Column + Clone> Column<Item> for FastFieldReaderCodecWrapper<Item, C> {
    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    // `maxdoc`.
    fn get_val(&self, idx: u64) -> Item {
        self.get_u64(idx)
    }

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// Regardless of the type of `Item`, this method works
    /// - transmuting the output array
    /// - extracting the `Item`s as if they were `u64`
    /// - possibly converting the `u64` value to the right type.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, start: u64, output: &mut [Item]) {
        self.get_range_u64(start, output);
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    fn min_value(&self) -> Item {
        Item::from_u64(self.reader.min_value())
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    fn max_value(&self) -> Item {
        Item::from_u64(self.reader.max_value())
    }

    fn num_vals(&self) -> u64 {
        self.reader.num_vals()
    }
}
