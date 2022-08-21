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

use std::path::Path;

use fastfield_codecs::FastFieldCodecReader;
use fastfield_codecs::FastFieldCodec;
use fastfield_codecs::dynamic::DynamicFastFieldReader;

use crate::directory::CompositeFile;
use crate::directory::RamDirectory;
use crate::directory::WritePtr;
use crate::fastfield::FastValue;
use crate::schema::Schema;

/// Wrapper for accessing a fastfield.
///
/// Holds the data and the codec to the read the data.
#[derive(Clone)]
pub struct FastFieldReaderCodecWrapper<Item: FastValue, CodecReader> {
    reader: CodecReader,
    _phantom: PhantomData<Item>,
}

impl<Item: FastValue, C: FastFieldCodecReader + Clone> FastFieldReader<Item>
    for FastFieldReaderCodecWrapper<Item, C>
{
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

impl<Item: FastValue, Codec: FastFieldCodec> FastFieldReaderCodecWrapper<Item, Codec> {
    // /// Opens a fast field given a file.
    // pub fn open(file: FileSlice) -> crate::Result<Self> {
    //     let mut bytes = file.read_bytes()?;
    //     Self::open_from_bytes(bytes)
    // }

    /// Opens a fast field given the bytes.
    pub fn open_from_bytes(bytes: OwnedBytes) -> crate::Result<Self> {
        let reader = C::open_from_bytes(bytes)?;
        Ok(FastFieldReaderCodecWrapper {
            reader,
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

// impl<Item: FastValue> From<Vec<Item>> for DynamicFastFieldReader<Item> {
//     fn from(vals: Vec<Item>) -> DynamicFastFieldReader<Item> {
//         let mut schema_builder = Schema::builder();
//         let field = schema_builder.add_u64_field("field", FAST);
//         let schema = schema_builder.build();
//         let path = Path::new("__dummy__");
//         let directory: RamDirectory = RamDirectory::create();
//         {
//             let write: WritePtr = directory
//                 .open_write(path)
//                 .expect("With a RamDirectory, this should never fail.");
//             let mut serializer = CompositeFastFieldSerializer::from_write(write)
//                 .expect("With a RamDirectory, this should never fail.");
//             let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
//             {
//                 let fast_field_writer = fast_field_writers
//                     .get_field_writer_mut(field)
//                     .expect("With a RamDirectory, this should never fail.");
//                 for val in vals {
//                     fast_field_writer.add_val(val.to_u64());
//                 }
//             }
//             fast_field_writers
//                 .serialize(&mut serializer, &HashMap::new(), None)
//                 .unwrap();
//             serializer.close().unwrap();
//         }

//         let file = directory.open_read(path).expect("Failed to open the file");
//         let composite_file = CompositeFile::open(&file).expect("Failed to read the composite file");
//         let field_file = composite_file
//             .open_read(field)
//             .expect("File component not found");
//         DynamicFastFieldReader::open(field_file).unwrap()
//     }
// }

