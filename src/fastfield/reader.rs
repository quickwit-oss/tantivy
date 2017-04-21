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


lazy_static! {
    static ref U64_FAST_FIELD_EMPTY: ReadOnlySource = {
        let u64_fast_field = U64FastFieldReader::from(Vec::new());
        u64_fast_field._data.clone()
    };
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

    /// Opens a new fast field reader given a read only source.
    ///
    /// # Panics
    /// Panics if the data is corrupted.
    pub fn open(data: ReadOnlySource) -> U64FastFieldReader {
        
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

    pub fn get(&self, doc: DocId) -> u64 {
        self.min_val + self.bit_unpacker.get(doc as usize)
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
        let fast_field_readers = U64FastFieldsReader::open(source).unwrap();
        fast_field_readers.get_field(field).unwrap()
     }
}

pub struct U64FastFieldsReader {
    source: ReadOnlySource,
    field_offsets: HashMap<Field, (u32, u32)>,
}


unsafe impl Send for U64FastFieldsReader {}
unsafe impl Sync for U64FastFieldsReader {}

impl U64FastFieldsReader {
    pub fn open(source: ReadOnlySource) -> io::Result<U64FastFieldsReader> {
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
        Ok(U64FastFieldsReader {
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
    pub fn get_field(&self, field: Field) -> Option<U64FastFieldReader> {
        self.field_offsets
            .get(&field)
            .map(|&(start, stop)| {
                let field_source = self.source.slice(start as usize, stop as usize);
                U64FastFieldReader::open(field_source)
            })
    }
}
