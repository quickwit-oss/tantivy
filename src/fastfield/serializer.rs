use common::BinarySerializable;
use directory::WritePtr;
use schema::Field;
use common::bitpacker::{compute_num_bits, BitPacker};
use common::CountingWriter;
use std::io::{self, Write, Seek, SeekFrom};

/// `FastFieldSerializer` is in charge of serializing
/// fastfields on disk.
///
/// Fast fields are encoded using bit-packing.
///
/// `FastFieldWriter`s are in charge of pushing the data to
/// the serializer.
/// The serializer expects to receive the following calls.
///
/// * `new_u64_fast_field(...)`
/// * `add_val(...)`
/// * `add_val(...)`
/// * `add_val(...)`
/// * ...
/// * `close_field()`
/// * `new_u64_fast_field(...)`
/// * `add_val(...)`
/// * ...
/// * `close_field()`
/// * `close()`
pub struct FastFieldSerializer {
    write: CountingWriter<WritePtr>,
    fields: Vec<(Field, u32)>,
    min_value: u64,
    field_open: bool,
    bit_packer: BitPacker,
}


impl FastFieldSerializer {
    /// Constructor
    pub fn new(write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let mut counting_writer = CountingWriter::wrap(write);
        0u32.serialize(&mut counting_writer)?;
        Ok(FastFieldSerializer {
               write: counting_writer,
               fields: Vec::new(),
               min_value: 0,
               field_open: false,
               bit_packer: BitPacker::new(0),
           })
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field(&mut self,
                              field: Field,
                              min_value: u64,
                              max_value: u64)
                              -> io::Result<()> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Previous field not closed"));
        }
        self.min_value = min_value;
        self.field_open = true;
        self.fields.push((field, self.write.written_bytes() as u32));
        let write = &mut self.write;
        min_value.serialize(write)?;
        let amplitude = max_value - min_value;
        amplitude.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        self.bit_packer = BitPacker::new(num_bits as usize);
        Ok(())
    }


    /// Pushes a new value to the currently open u64 fast field.
    pub fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer.write(val_to_write, &mut self.write)?;
        Ok(())
    }

    /// Close the u64 fast field.
    pub fn close_field(&mut self) -> io::Result<()> {
        if !self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Current field is already closed"));
        }
        self.field_open = false;
        // adding some padding to make sure we
        // can read the last elements with our u64
        // cursor
        self.bit_packer.close(&mut self.write)?;
        Ok(())
    }


    /// Closes the serializer
    ///
    /// After this call the data must be persistently save on disk.
    pub fn close(self) -> io::Result<usize> {
        if self.field_open {
            return Err(io::Error::new(io::ErrorKind::Other, "Last field not closed"));
        }
        let header_offset: usize = self.write.written_bytes() as usize;
        let (mut write, written_size) = self.write.finish()?;
        self.fields.serialize(&mut write)?;
        write.seek(SeekFrom::Start(0))?;
        (header_offset as u32).serialize(&mut write)?;
        write.flush()?;
        Ok(written_size)
    }
}
