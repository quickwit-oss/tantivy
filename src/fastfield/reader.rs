use std::io;
use std::io::{SeekFrom, Seek};
use std::collections::HashMap;
use std::ops::Deref;

use directory::ReadOnlySource;
use fastfield::DividerU32;
use common::BinarySerializable;
use DocId;
use schema::Field;

use super::compute_num_bits;

pub struct U32FastFieldReader {
    _data: ReadOnlySource,
    data_ptr: *const u64,
    min_val: u32,
    max_val: u32,
    num_bits: u8,
    mask: u32,
    num_in_pack: u32,
    divider: DividerU32,
}

impl U32FastFieldReader {

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
            let mut cursor = data.cursor();
            min_val = try!(u32::deserialize(&mut cursor));
            amplitude = try!(u32::deserialize(&mut cursor));
        }
        let num_bits = compute_num_bits(amplitude);
        let mask = (1 << num_bits) - 1;
        let num_in_pack;
        if num_bits == 0u8 {
            num_in_pack = 0u32;
        }
        else {
            num_in_pack = 64u32 / (num_bits as u32);
        }
        let ptr: *const u8 = &(data.deref()[8 as usize]);
        Ok(U32FastFieldReader {
            _data: data,
            data_ptr: ptr as *const u64,
            min_val: min_val,
            max_val: min_val + amplitude,
            num_bits: num_bits,
            mask: mask,
            num_in_pack: num_in_pack,
            divider: DividerU32::divide_by(num_in_pack),
        })
    }

    pub fn get(&self, doc: DocId) -> u32 {
        if self.num_in_pack == 0u32 {
            return self.min_val;
        }
        let long_addr = self.divider.divide(doc);
        let ord_within_long = doc - long_addr * self.num_in_pack;
        let bit_shift = (self.num_bits as u32) * ord_within_long;
        let val_unshifted_unmasked: u64 = unsafe { *self.data_ptr.offset(long_addr as isize) };
        let val_shifted = (val_unshifted_unmasked >> bit_shift) as u32;
        return self.min_val + (val_shifted & self.mask);
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
            let mut cursor = source.cursor();
            header_offset = try!(u32::deserialize(&mut cursor));
            try!(cursor.seek(SeekFrom::Start(header_offset as u64)));
            field_offsets = try!(Vec::deserialize(&mut cursor));
        }
        let mut end_offsets: Vec<u32> = field_offsets
            .iter()
            .map(|&(_, offset)| offset.clone())
            .collect();
        end_offsets.push(header_offset);
        let mut field_offsets_map: HashMap<Field, (u32, u32)> = HashMap::new();
        for (field_start_offsets, stop_offset) in field_offsets.iter().zip(end_offsets.iter().skip(1)) {
            let (field, start_offset) = field_start_offsets.clone();
            field_offsets_map.insert(field.clone(), (start_offset.clone(), stop_offset.clone()));
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
