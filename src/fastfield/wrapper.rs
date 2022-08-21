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
//

use std::marker::PhantomData;

use fastfield_codecs::dynamic::DynamicFastFieldCodec;
use fastfield_codecs::{FastFieldCodec, FastFieldCodecReader, FastFieldStats};
use ownedbytes::OwnedBytes;

use crate::directory::FileSlice;
use crate::fastfield::{FastFieldReader, FastFieldReaderImpl, FastValue};
use crate::DocId;

/// Wrapper for accessing a fastfield.
///
/// Holds the data and the codec to the read the data.
pub struct FastFieldReaderWrapper<Item: FastValue, Codec: FastFieldCodec> {
    reader: Codec::Reader,
    _phantom: PhantomData<Item>,
    _codec: PhantomData<Codec>,
}

impl<Item: FastValue, Codec: FastFieldCodec> FastFieldReaderWrapper<Item, Codec> {
    fn new(reader: Codec::Reader) -> Self {
        Self {
            reader,
            _phantom: PhantomData,
            _codec: PhantomData,
        }
    }
}

impl<Item: FastValue, Codec: FastFieldCodec> Clone for FastFieldReaderWrapper<Item, Codec>
where Codec::Reader: Clone
{
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            _phantom: PhantomData,
            _codec: PhantomData,
        }
    }
}

impl<Item: FastValue, C: FastFieldCodec> FastFieldReader<Item> for FastFieldReaderWrapper<Item, C> {
    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    // `maxdoc`.
    fn get(&self, doc: DocId) -> Item {
        self.get_u64(u64::from(doc))
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
}

impl<Item: FastValue, Codec: FastFieldCodec> FastFieldReaderWrapper<Item, Codec> {
    /// Opens a fast field given a file.
    pub fn open(file: FileSlice) -> crate::Result<Self> {
        let mut bytes = file.read_bytes()?;
        // TODO
        // let codec_id = bytes.read_u8();
        // assert_eq!(
        //     0u8, codec_id,
        //     "Tried to open fast field as bitpacked encoded (id=1), but got serializer with \
        //      different id"
        // );
        Self::open_from_bytes(bytes)
    }

    /// Opens a fast field given the bytes.
    pub fn open_from_bytes(bytes: OwnedBytes) -> crate::Result<Self> {
        let reader = Codec::open_from_bytes(bytes)?;
        Ok(FastFieldReaderWrapper {
            reader,
            _codec: PhantomData,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub(crate) fn get_u64(&self, doc: u64) -> Item {
        let data = self.reader.get_u64(doc);
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

use itertools::Itertools;

impl<Item: FastValue, Arr: AsRef<[Item]>> From<Arr> for FastFieldReaderImpl<Item> {
    fn from(vals: Arr) -> FastFieldReaderImpl<Item> {
        let mut buffer = Vec::new();
        let vals_u64: Vec<u64> = vals.as_ref().iter().map(|val| val.to_u64()).collect();
        let (min_value, max_value) = vals_u64
            .iter()
            .copied()
            .minmax()
            .into_option()
            .expect("Expected non empty");
        let stats = FastFieldStats {
            min_value,
            max_value,
            num_vals: vals_u64.len() as u64,
        };
        DynamicFastFieldCodec
            .serialize(&mut buffer, &vals_u64, stats)
            .unwrap();
        let bytes = OwnedBytes::new(buffer);
        let fast_field_reader = DynamicFastFieldCodec::open_from_bytes(bytes).unwrap();
        FastFieldReaderImpl::new(fast_field_reader)
    }
}
