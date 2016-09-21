use common::BinarySerializable;
use directory::WritePtr;
use schema::Field;
use std::io;
use std::io::{SeekFrom, Write};
use super::compute_num_bits;


/// `FastFieldSerializer` is in charge of serializing
/// a fastfield on disk.
/// 
/// FastField are encoded using bit-packing.
/// 
/// `FastFieldWriter`s are in charge of pushing the data to
/// the serializer.
/// The serializer expects to receive the following calls.
///
/// * `new_u32_fast_field(...)`
/// * `add_val(...)`
/// * `add_val(...)`
/// * `add_val(...)`
/// * ...
/// * `close_field()`
/// * `new_u32_fast_field(...)`
/// * `add_val(...)`
/// * ...
/// * `close_field()`
/// * `close()`
pub struct FastFieldSerializer {
    write: WritePtr,
    written_size: usize,
    fields: Vec<(Field, u32)>,
    num_bits: u8,
    min_value: u32,
    field_open: bool,
    
    mini_buffer_written: usize,
    mini_buffer: u32,
}



impl FastFieldSerializer {
    /// Constructor
    pub fn new(mut write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let written_size: usize = try!(0u32.serialize(&mut write));
        Ok(FastFieldSerializer {
            write: write,
            written_size: written_size,
            fields: Vec::new(),
            num_bits: 0u8,
            min_value: 0,
            field_open: false,
            
            mini_buffer_written: 0,
            mini_buffer: 0u32,
        })
    }
    
    /// Start serializing a new u32 fast field
    pub fn new_u32_fast_field(&mut self, field: Field, min_value: u32, max_value: u32) -> io::Result<()> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Previous field not closed"));
        }
        self.min_value = min_value;
        self.field_open = true;
        self.fields.push((field, self.written_size as u32));
        let write: &mut Write = &mut self.write;
        self.written_size += try!(min_value.serialize(write));
        let amplitude = max_value - min_value;
        self.written_size += try!(amplitude.serialize(write));
        self.num_bits = compute_num_bits(amplitude);
        if self.num_bits == 0 {
            // if num_bits == 0 we make sure that we still write one mini buffer
            // so that the reader code does not overflows and does not
            // contain a needless if statement.
            self.mini_buffer_written += 1;
        }
        Ok(())
    }


    /// Pushes a new value to the currently open u32 fast field. 
    pub fn add_val(&mut self, val: u32) -> io::Result<()> {
        let write: &mut Write = &mut self.write;
        let val_to_write: u32 = val - self.min_value;
        if self.mini_buffer_written + self.num_bits as usize > 32 {
            self.mini_buffer |= val_to_write.wrapping_shl(self.mini_buffer_written as u32);
            self.written_size += try!(self.mini_buffer.serialize(write));
            // overflow of the shift operand is guarded here by the if case.
            self.mini_buffer = val_to_write.wrapping_shr(32u32 - self.mini_buffer_written as u32);
            self.mini_buffer_written = self.mini_buffer_written + (self.num_bits as usize) - 32 ;
        }
        else {
            self.mini_buffer |= val_to_write << self.mini_buffer_written;
            self.mini_buffer_written += self.num_bits as usize;
            if self.mini_buffer_written == 32 {
                self.written_size += try!(self.mini_buffer.serialize(write));
                self.mini_buffer_written = 0;
                self.mini_buffer = 0u32;
            }    
        }
        Ok(())
    }
    
    /// Close the u32 fast field. 
    pub fn close_field(&mut self,) -> io::Result<()> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Current field is already closed"));
        }
        self.field_open = false;
        if self.mini_buffer_written > 0 {
            self.mini_buffer_written = 0;
            self.written_size += try!(self.mini_buffer.serialize(&mut self.write));
        }
        // adding some padding to make sure we
        // can read the last elements with our u64
        // cursor
        self.written_size += try!(0u32.serialize(&mut self.write));
        self.mini_buffer = 0;
        Ok(())
    }
    
    
    /// Closes the serializer
    /// 
    /// After this call the data must be persistently save on disk.
    pub fn close(mut self,) -> io::Result<usize> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Last field not closed"));
        }
        let header_offset: usize = self.written_size;
        self.written_size += try!(self.fields.serialize(&mut self.write));
        try!(self.write.seek(SeekFrom::Start(0)));
        try!((header_offset as u32).serialize(&mut self.write));
        try!(self.write.flush());
        Ok(self.written_size)
    }
}
