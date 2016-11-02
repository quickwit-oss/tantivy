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
use super::compute_num_bits;


lazy_static! {
    static ref U32_FAST_FIELD_EMPTY: ReadOnlySource = {
        let u32_fast_field = U32FastFieldReader::from(Vec::new());
        u32_fast_field._data.clone()
    };
}

pub struct U32FastFieldReader {
    _data: ReadOnlySource,
    data_ptr: *const u8,
    min_val: u32,
    max_val: u32,
    num_bits: u32,
    mask: u32,
}

impl U32FastFieldReader {

    pub fn empty() -> U32FastFieldReader {
        U32FastFieldReader::open(U32_FAST_FIELD_EMPTY.clone()).expect("should always work.")
    }

    pub fn min_val(&self,) -> u32 {
        self.min_val
    }

    pub fn max_val(&self,) -> u32 {
        self.max_val
    }

    pub fn open(data: ReadOnlySource) -> io::Result<U32FastFieldReader> {
        let min_val;
        let amplitude;
        {
            let mut cursor = data.as_slice();
            min_val = try!(u32::deserialize(&mut cursor));
            amplitude = try!(u32::deserialize(&mut cursor));
        }
        let num_bits = compute_num_bits(amplitude);
        let mask = (1 << num_bits) - 1;
        let ptr: *const u8 = &(data.deref()[8 as usize]);
        Ok(U32FastFieldReader {
            _data: data,
            data_ptr: ptr,
            min_val: min_val,
            max_val: min_val + amplitude,
            num_bits: num_bits as u32,
            mask: mask,
        })
    }

    pub fn get(&self, doc: DocId) -> u32 {
        if self.num_bits == 0u32 {
            return self.min_val;
        }
        let addr = (doc * self.num_bits) / 8;
        let bit_shift = (doc * self.num_bits) - addr * 8; //doc - long_addr * self.num_in_pack;
        let val_unshifted_unmasked: u64 = unsafe { * (self.data_ptr.offset(addr as isize) as *const u64) };
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u32;
        self.min_val + (val_shifted & self.mask)
        
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
    
    pub fn get_field(&self, field: Field) -> io::Result<U32FastFieldReader> {
        match self.field_offsets.get(&field) {
            Some(&(start, stop)) => {
                let field_source = self.source.slice(start as usize, stop as usize);
                U32FastFieldReader::open(field_source)
            }
            None => {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "Could not find field"))
            }

        }

    }
}
