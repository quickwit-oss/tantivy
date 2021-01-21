use super::FastValue;
use crate::common::bitpacker::BitUnpacker;
use crate::common::compute_num_bits;
use crate::common::BinarySerializable;
use crate::common::CompositeFile;
use crate::directory::FileSlice;
use crate::directory::{Directory, RAMDirectory, WritePtr};
use crate::fastfield::{FastFieldSerializer, FastFieldsWriter};
use crate::schema::Schema;
use crate::schema::FAST;
use crate::DocId;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;

/// Trait for accessing a fastfield.
///
/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct FastFieldReader<Item: FastValue> {
    bit_unpacker: BitUnpacker,
    min_value_u64: u64,
    max_value_u64: u64,
    _phantom: PhantomData<Item>,
}

impl<Item: FastValue> FastFieldReader<Item> {
    /// Opens a fast field given a file.
    pub fn open(file: FileSlice) -> crate::Result<Self> {
        let mut bytes = file.read_bytes()?;
        let min_value = u64::deserialize(&mut bytes)?;
        let amplitude = u64::deserialize(&mut bytes)?;
        let max_value = min_value + amplitude;
        let num_bits = compute_num_bits(amplitude);
        let bit_unpacker = BitUnpacker::new(bytes, num_bits);
        Ok(FastFieldReader {
            min_value_u64: min_value,
            max_value_u64: max_value,
            bit_unpacker,
            _phantom: PhantomData,
        })
    }

    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    // `maxdoc`.
    pub fn get(&self, doc: DocId) -> Item {
        self.get_u64(u64::from(doc))
    }

    pub(crate) fn get_u64(&self, doc: u64) -> Item {
        Item::from_u64(self.min_value_u64 + self.bit_unpacker.get(doc))
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
    pub fn get_range(&self, start: DocId, output: &mut [Item]) {
        self.get_range_u64(u64::from(start), output);
    }

    /// Returns the minimum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn min_value(&self) -> Item {
        Item::from_u64(self.min_value_u64)
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn max_value(&self) -> Item {
        Item::from_u64(self.max_value_u64)
    }
}

impl<Item: FastValue> From<Vec<Item>> for FastFieldReader<Item> {
    fn from(vals: Vec<Item>) -> FastFieldReader<Item> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("__dummy__");
        let directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory
                .open_write(path)
                .expect("With a RAMDirectory, this should never fail.");
            let mut serializer = FastFieldSerializer::from_write(write)
                .expect("With a RAMDirectory, this should never fail.");
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            {
                let fast_field_writer = fast_field_writers
                    .get_field_writer(field)
                    .expect("With a RAMDirectory, this should never fail.");
                for val in vals {
                    fast_field_writer.add_val(val.to_u64());
                }
            }
            fast_field_writers
                .serialize(&mut serializer, &HashMap::new())
                .unwrap();
            serializer.close().unwrap();
        }

        let file = directory.open_read(path).expect("Failed to open the file");
        let composite_file = CompositeFile::open(&file).expect("Failed to read the composite file");
        let field_file = composite_file
            .open_read(field)
            .expect("File component not found");
        FastFieldReader::open(field_file).unwrap()
    }
}
