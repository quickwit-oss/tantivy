use directory::ReadOnlySource;
use common::{self, BinarySerializable};
use common::bitpacker::{compute_num_bits, BitUnpacker};
use DocId;
use schema::SchemaBuilder;
use std::path::Path;
use schema::FAST;
use directory::{WritePtr, RAMDirectory, Directory};
use fastfield::{FastFieldSerializer, FastFieldsWriter};
use schema::FieldType;
use std::mem;
use common::CompositeFile;
use owning_ref::OwningRef;

/// Trait for accessing a fastfield.
///
/// Depending on the field type, a different
/// fast field is required.
pub trait FastFieldReader: Sized {
    /// Type of the value stored in the fastfield.
    type ValueType;

    /// Return the value associated to the given document.
    ///
    /// This accessor should return as fast as possible.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    // `maxdoc`.
    fn get(&self, doc: DocId) -> Self::ValueType;

    /// Fills an output buffer with the fast field values
    /// associated with the `DocId` going from
    /// `start` to `start + output.len()`.
    ///
    /// # Panics
    ///
    /// May panic if `start + output.len()` is greater than
    /// the segment's `maxdoc`.
    fn get_range(&self, start: u32, output: &mut [Self::ValueType]);

    /// Opens a fast field given a source.
    fn open(source: ReadOnlySource) -> Self;

    /// Returns true iff the given field_type makes
    /// it possible to access the field values via a
    /// fastfield.
    fn is_enabled(field_type: &FieldType) -> bool;
}

/// `FastFieldReader` for unsigned 64-bits integers.
pub struct U64FastFieldReader {
    bit_unpacker: BitUnpacker<OwningRef<ReadOnlySource, [u8]>>,
    min_value: u64,
    max_value: u64,
}

impl U64FastFieldReader {
    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual minimum value.
    pub fn min_value(&self) -> u64 {
        self.min_value
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn max_value(&self) -> u64 {
        self.max_value
    }
}

impl FastFieldReader for U64FastFieldReader {
    type ValueType = u64;

    fn get(&self, doc: DocId) -> u64 {
        self.min_value + self.bit_unpacker.get(doc as usize)
    }

    fn is_enabled(field_type: &FieldType) -> bool {
        match *field_type {
            FieldType::U64(ref integer_options) => integer_options.is_fast(),
            _ => false,
        }
    }

    fn get_range(&self, start: u32, output: &mut [Self::ValueType]) {
        self.bit_unpacker.get_range(start, output);
        for out in output.iter_mut() {
            *out += self.min_value;
        }
    }

    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    fn open(data: ReadOnlySource) -> U64FastFieldReader {
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
        let bit_unpacker = BitUnpacker::new(owning_ref, num_bits as usize);
        U64FastFieldReader {
            min_value: min_value,
            max_value: max_value,
            bit_unpacker: bit_unpacker,
        }
    }
}


impl From<Vec<u64>> for U64FastFieldReader {
    fn from(vals: Vec<u64>) -> U64FastFieldReader {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("__dummy__");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(path).expect("With a RAMDirectory, this should never fail.");
            let mut serializer = FastFieldSerializer::from_write(write).expect("With a RAMDirectory, this should never fail.");
            let mut fast_field_writers = FastFieldsWriter::from_schema(&schema);
            {
                let fast_field_writer = fast_field_writers.get_field_writer(field).expect("With a RAMDirectory, this should never fail.");
                for val in vals {
                    fast_field_writer.add_val(val);
                }
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }

        let source = directory.open_read(path).expect("Failed to open the file");
        let composite_file =
            CompositeFile::open(source).expect("Failed to read the composite file");

        let field_source = composite_file
            .open_read(field)
            .expect("File component not found");
        U64FastFieldReader::open(field_source)
    }
}

/// `FastFieldReader` for signed 64-bits integers.
pub struct I64FastFieldReader {
    underlying: U64FastFieldReader,
}

impl I64FastFieldReader {
    /// Returns the minimum value for this fast field.
    ///
    /// The min value does not take in account of possible
    /// deleted document, and should be considered as a lower bound
    /// of the actual minimum value.
    pub fn min_value(&self) -> i64 {
        common::u64_to_i64(self.underlying.min_value())
    }

    /// Returns the maximum value for this fast field.
    ///
    /// The max value does not take in account of possible
    /// deleted document, and should be considered as an upper bound
    /// of the actual maximum value.
    pub fn max_value(&self) -> i64 {
        common::u64_to_i64(self.underlying.max_value())
    }
}

impl FastFieldReader for I64FastFieldReader {
    type ValueType = i64;

    ///
    ///
    /// # Panics
    ///
    /// May panic or return wrong random result if `doc`
    /// is greater or equal to the segment's `maxdoc`.
    fn get(&self, doc: DocId) -> i64 {
        common::u64_to_i64(self.underlying.get(doc))
    }

    ///
    /// # Panics
    ///
    /// May panic or return wrong random result if `doc`
    /// is greater or equal to the segment's `maxdoc`.
    fn get_range(&self, start: u32, output: &mut [Self::ValueType]) {
        let output_u64: &mut [u64] = unsafe { mem::transmute(output) };
        self.underlying.get_range(start, output_u64);
        for mut_val in output_u64.iter_mut() {
            *mut_val = common::u64_to_i64(*mut_val as u64) as u64;
        }
    }

    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    fn open(data: ReadOnlySource) -> I64FastFieldReader {
        I64FastFieldReader { underlying: U64FastFieldReader::open(data) }
    }

    fn is_enabled(field_type: &FieldType) -> bool {
        match *field_type {
            FieldType::I64(ref integer_options) => integer_options.is_fast(),
            _ => false,
        }
    }
}
