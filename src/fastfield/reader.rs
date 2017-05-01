use std::io;
use std::collections::HashMap;
use directory::ReadOnlySource;
use common::BinarySerializable;
use DocId;
use schema::{Field, SchemaBuilder};
use std::path::Path;
use schema::FAST;
use directory::{WritePtr, RAMDirectory, Directory};
use fastfield::FastFieldSerializer;
use fastfield::U64FastFieldsWriter;
use common::bitpacker::compute_num_bits;
use common::bitpacker::BitUnpacker;
use schema::FieldType;

lazy_static! {
    static ref U64_FAST_FIELD_EMPTY: ReadOnlySource = {
        let u64_fast_field = U64FastFieldReader::from(Vec::new());
        u64_fast_field._data.clone()
    };
}


pub trait FastFieldReader: Sized {
    type ValueType;

    fn get(&self, doc: DocId) -> Self::ValueType;

    fn open(source: ReadOnlySource) -> Self;

    fn is_enabled(field_type: &FieldType) -> bool;
}

pub struct U64FastFieldReader {
    _data: ReadOnlySource,
    bit_unpacker: BitUnpacker,
    min_val: u64,
    max_val: u64,
}

impl U64FastFieldReader {

    pub fn empty() -> U64FastFieldReader {
        U64FastFieldReader::open(U64_FAST_FIELD_EMPTY.clone())
    }

    pub fn min_val(&self,) -> u64 {
        self.min_val
    }

    pub fn max_val(&self,) -> u64 {
        self.max_val
    }
}

impl FastFieldReader for U64FastFieldReader {
    type ValueType = u64;
    
    fn get(&self, doc: DocId) -> u64 {
        self.min_val + self.bit_unpacker.get(doc as usize)
    }

    fn is_enabled(field_type: &FieldType) -> bool {
        match field_type {
            &FieldType::U64(ref integer_options) => {
                if integer_options.is_fast() {
                    true
                }
                else {
                    false
                }
            },
            _ => false,
        }
    }

    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    fn open(data: ReadOnlySource) -> U64FastFieldReader {
        let min_val: u64;
        let max_val: u64;
        let bit_unpacker: BitUnpacker;
        
        {
            let mut cursor: &[u8] = data.as_slice();
            min_val = u64::deserialize(&mut cursor).expect("Failed to read the min_val of fast field.");
            let amplitude = u64::deserialize(&mut cursor).expect("Failed to read the amplitude of fast field.");
            max_val = min_val + amplitude;
            let num_bits = compute_num_bits(amplitude);
            bit_unpacker = BitUnpacker::new(cursor, num_bits as usize)
        }

        U64FastFieldReader {
            _data: data,
            bit_unpacker: bit_unpacker,
            min_val: min_val,
            max_val: max_val,
        }
    }

}


impl From<Vec<u64>> for U64FastFieldReader {
    fn from(vals: Vec<u64>) -> U64FastFieldReader {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_u64_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U64FastFieldsWriter::from_schema(&schema);
            for val in vals {
                let mut fast_field_writer = fast_field_writers.get_field_writer(field).unwrap();
                fast_field_writer.add_val(val);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let fast_field_readers = FastFieldsReader::open(source).unwrap();
        fast_field_readers.open_reader(field).unwrap()
     }
}



pub struct I64FastFieldReader {
    _data: ReadOnlySource,
    bit_unpacker: BitUnpacker,
    min_val: i64,
    max_val: i64,
}

impl I64FastFieldReader {

    pub fn empty() -> I64FastFieldReader {
        // TODO implement
        panic!("");
        // I64FastFieldReader::open(I64_FAST_FIELD_EMPTY.clone())
    }

    pub fn min_val(&self,) -> i64 {
        self.min_val
    }

    pub fn max_val(&self,) -> i64 {
        self.max_val
    }
}

impl FastFieldReader for I64FastFieldReader {
    type ValueType = i64;
    
    fn get(&self, doc: DocId) -> i64 {
        self.min_val + (self.bit_unpacker.get(doc as usize) as i64)
    }
    
    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    fn open(data: ReadOnlySource) -> I64FastFieldReader {
        let min_val: i64;
        let max_val: i64;
        let bit_unpacker: BitUnpacker;
        
        {
            let mut cursor: &[u8] = data.as_slice();
            min_val = i64::deserialize(&mut cursor).expect("Failed to read the min_val of fast field.");
            let amplitude = u64::deserialize(&mut cursor).expect("Failed to read the amplitude of fast field.");
            max_val = min_val + (amplitude as i64);
            let num_bits = compute_num_bits(amplitude);
            bit_unpacker = BitUnpacker::new(cursor, num_bits as usize)
        }

        I64FastFieldReader {
            _data: data,
            bit_unpacker: bit_unpacker,
            min_val: min_val,
            max_val: max_val,
        }
    }


    fn is_enabled(field_type: &FieldType) -> bool {
        match field_type {
            &FieldType::I64(ref integer_options) => {
                if integer_options.is_fast() {
                    true
                }
                else {
                    false
                }
            },
            _ => false,
        }
    }

}




pub struct FastFieldsReader {
    source: ReadOnlySource,
    field_offsets: HashMap<Field, (u32, u32)>,
}

impl FastFieldsReader {

    pub fn open(source: ReadOnlySource) -> io::Result<FastFieldsReader> {
        let header_offset;
        let field_offsets: Vec<(Field, u32)>;
        {
            let buffer = source.as_slice();
            {
                let mut cursor = buffer;
                header_offset = try!(u32::deserialize(&mut cursor));
            }
            {
                let mut cursor = &buffer[header_offset as usize..];
                field_offsets = try!(Vec::deserialize(&mut cursor));    
            }
        }
        let mut end_offsets: Vec<u32> = field_offsets
            .iter()
            .map(|&(_, offset)| offset)
            .collect();
        end_offsets.push(header_offset);
        let mut field_offsets_map: HashMap<Field, (u32, u32)> = HashMap::new();
        for (field_start_offsets, stop_offset) in field_offsets.iter().zip(end_offsets.iter().skip(1)) {
            let (field, start_offset) = *field_start_offsets;
            field_offsets_map.insert(field, (start_offset, *stop_offset));
        }
        Ok(FastFieldsReader {
            field_offsets: field_offsets_map,
            source: source,
        })
    }

    /// Returns the u64 fast value reader if the field
    /// is a u64 field indexed as "fast".
    ///
    /// Return None if the field is not a u64 field
    /// indexed with the fast option.
    ///
    /// # Panics
    /// May panic if the index is corrupted.
    pub fn open_reader<FFReader: FastFieldReader>(&self, field: Field) -> Option<FFReader> {
        self.field_offsets
            .get(&field)
            .map(|&(start, stop)| {
                let field_source = self.source.slice(start as usize, stop as usize);
                FFReader::open(field_source)
            })
    }
}
