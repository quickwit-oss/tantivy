use common::BinarySerializable;
use directory::WritePtr;
use schema::Field;
use std::io;
use std::io::{SeekFrom, Write};
use super::compute_num_bits;

pub struct FastFieldSerializer {
    write: WritePtr,
    written_size: usize,
    fields: Vec<(Field, u32)>,
    num_bits: u8,

    min_value: u32,

    field_open: bool,
    mini_buffer_written: usize,
    mini_buffer: u64,
}

impl FastFieldSerializer {
    pub fn new(mut write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let written_size: usize = try!(0u32.serialize(&mut write));
        Ok(FastFieldSerializer {
            write: write,
            written_size: written_size,
            fields: Vec::new(),
            num_bits: 0u8,
            field_open: false,
            mini_buffer_written: 0,
            mini_buffer: 0,
            min_value: 0,
        })
    }

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
        Ok(())
    }

    pub fn add_val(&mut self, val: u32) -> io::Result<()> {
        let write: &mut Write = &mut self.write;
        if self.mini_buffer_written + (self.num_bits as usize) > 64 {
            self.written_size += try!(self.mini_buffer.serialize(write));
            self.mini_buffer = 0;
            self.mini_buffer_written = 0;
        }
        self.mini_buffer |= ((val - self.min_value) as u64) << self.mini_buffer_written;
        self.mini_buffer_written += self.num_bits as usize;
        Ok(())
    }

    pub fn close_field(&mut self,) -> io::Result<()> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Current field is already closed"));
        }
        self.field_open = false;
        if self.mini_buffer_written > 0 {
            self.mini_buffer_written = 0;
            self.written_size += try!(self.mini_buffer.serialize(&mut self.write));
        }
        self.mini_buffer = 0;
        Ok(())
    }

    pub fn close(mut self,) -> io::Result<usize> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Last field not closed"));
        }
        let header_offset: usize = self.written_size;
        self.written_size += try!(self.fields.serialize(&mut self.write));
        try!(self.write.seek(SeekFrom::Start(0)));
        try!((header_offset as u32).serialize(&mut self.write));
        Ok(self.written_size)
    }
}
