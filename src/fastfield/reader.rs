use std::io;
use std::collections::HashMap;
use std::ops::Deref;

use directory::ReadOnlySource;
use common::BinarySerializable;
use DocId;
use schema::{Field, SchemaBuilder};
use std::path::Path;
use schema::FAST;
use directory::{WritePtr, RAMDirectory, Directory};
use fastfield::FastFieldSerializer;
use fastfield::U32FastFieldsWriter;
use common::bitpacker::compute_num_bits;
use common::bitpacker::BitUnpacker;


lazy_static! {
    static ref U32_FAST_FIELD_EMPTY: ReadOnlySource = {
        let u32_fast_field = U32FastFieldReader::from(Vec::new());
        u32_fast_field._data.clone()
    };
}

pub struct U32FastFieldReader {
    _data: ReadOnlySource,
    bit_unpacker: BitUnpacker,
    min_val: u32,
    max_val: u32,
}

impl U32FastFieldReader {

    pub fn empty() -> U32FastFieldReader {
        U32FastFieldReader::open(U32_FAST_FIELD_EMPTY.clone())
    }

    pub fn min_val(&self,) -> u32 {
        self.min_val
    }

    pub fn max_val(&self,) -> u32 {
        self.max_val
    }

    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    pub fn open(data: ReadOnlySource) -> U32FastFieldReader {
        let min_val;
        let amplitude;
        let max_val;
        {
            let mut cursor = data.as_slice();
            min_val = u32::deserialize(&mut cursor).unwrap();
            amplitude = u32::deserialize(&mut cursor).unwrap();
            max_val = min_val + amplitude;
        }
        let num_bits = compute_num_bits(amplitude);
        let bit_unpacker = {
            let data_arr = &(data.deref()[8..]);
            BitUnpacker::new(data_arr, num_bits as usize)
        };
        U32FastFieldReader {
            _data: data,
            bit_unpacker: bit_unpacker,
            min_val: min_val,
            max_val: max_val,
        }
    }

    pub fn get(&self, doc: DocId) -> u32 {
        self.min_val + self.bit_unpacker.get(doc as usize)
    }
}


impl From<Vec<u32>> for U32FastFieldReader {
    fn from(vals: Vec<u32>) -> U32FastFieldReader {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_u32_field("field", FAST);
        let schema = schema_builder.build();
        let path = Path::new("test");
        let mut directory: RAMDirectory = RAMDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            let mut fast_field_writers = U32FastFieldsWriter::from_schema(&schema);
            for val in vals {
                let mut fast_field_writer = fast_field_writers.get_field_writer(field).unwrap();
                fast_field_writer.add_val(val);
            }
            fast_field_writers.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let fast_field_readers = U32FastFieldsReader::open(source).unwrap();
        fast_field_readers.get_field(field).unwrap()
     }
}

pub struct U32FastFieldsReader {
    source: ReadOnlySource,
    field_offsets: HashMap<Field, (u32, u32)>,
}

unsafe impl Send for U32FastFieldReader {}
unsafe impl Sync for U32FastFieldReader {}

unsafe impl Send for U32FastFieldsReader {}
unsafe impl Sync for U32FastFieldsReader {}

impl U32FastFieldsReader {
    pub fn open(source: ReadOnlySource) -> io::Result<U32FastFieldsReader> {
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
        Ok(U32FastFieldsReader {
            field_offsets: field_offsets_map,
            source: source,
        })
    }
    
    /// Returns the u32 fast value reader if the field
    /// is a u32 field indexed as "fast".
    ///
    /// Return None if the field is not a u32 field
    /// indexed with the fast option.
    ///
    /// # Panics
    /// May panic if the index is corrupted.
    pub fn get_field(&self, field: Field) -> Option<U32FastFieldReader> {
        self.field_offsets
            .get(&field)
            .map(|&(start, stop)| {
                let field_source = self.source.slice(start as usize, stop as usize);
                U32FastFieldReader::open(field_source)
            })
    }
}
