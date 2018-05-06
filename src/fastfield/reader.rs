use super::FastValue;
use common::bitpacker::BitUnpacker;
use common::compute_num_bits;
use common::BinarySerializable;
use common::CompositeFile;
use directory::ReadOnlySource;
use directory::{Directory, RAMDirectory, WritePtr};
use fastfield::{FastFieldSerializer, FastFieldsWriter};
use owning_ref::OwningRef;
use schema::SchemaBuilder;
use schema::FAST;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::path::Path;
use DocId;

/// Trait for accessing a fastfield.
///
/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct FastFieldReader<Item: FastValue> {
    bit_unpacker: BitUnpacker<OwningRef<ReadOnlySource, [u8]>>,
    min_value_u64: u64,
    max_value_u64: u64,
    _phantom: PhantomData<Item>,
}

impl<Item: FastValue> FastFieldReader<Item> {
    /// Opens a fast field given a source.
    pub fn open(data: ReadOnlySource) -> Self {
        let min_value: u64;
        let amplitude: u64;
        {
            let mut cursor = data.as_slice();
            min_value =
                u64::deserialize(&mut cursor).expect("Failed to read the min_value of fast field.");
            amplitude =
                u64::deserialize(&mut cursor).expect("Failed to read the amplitude of fast field.");
        }
        let max_value = min_value + amplitude;
        let num_bits = compute_num_bits(amplitude);
        let owning_ref = OwningRef::new(data).map(|data| &data[16..]);
        let bit_unpacker = BitUnpacker::new(owning_ref, num_bits);
        FastFieldReader {
            min_value_u64: min_value,
            max_value_u64: max_value,
            bit_unpacker,
            _phantom: PhantomData,
        }
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
        Item::from_u64(self.min_value_u64 + self.bit_unpacker.get(doc as usize))
    }

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    ///
    // TODO change start to `u64`.
    // For multifastfield, start is an index in a second fastfield, not a `DocId`
    pub fn get_range(&self, start: u32, output: &mut [Item]) {
        let output_u64: &mut [u64] = unsafe { mem::transmute(output) };
        self.bit_unpacker.get_range(start, output_u64);
        for out in output_u64.iter_mut() {
            *out = Item::from_u64(*out + self.min_value_u64).as_u64();
        }
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
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("__dummy__");
        let mut directory: RAMDirectory = RAMDirectory::create();
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

        let source = directory.open_read(path).expect("Failed to open the file");
        let composite_file =
            CompositeFile::open(&source).expect("Failed to read the composite file");
        let field_source = composite_file
            .open_read(field)
            .expect("File component not found");
        FastFieldReader::open(field_source)
    }
}
