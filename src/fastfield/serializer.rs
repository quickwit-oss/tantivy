use common::BinarySerializable;
use directory::WritePtr;
use schema::Field;
use common::bitpacker::{compute_num_bits, BitPacker};
use common::CountingWriter;
use common::CompositeWrite;
use std::io::{self, Write};

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
    composite_write: CompositeWrite<WritePtr>,
}

impl FastFieldSerializer {
    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<FastFieldSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(FastFieldSerializer {
            composite_write: composite_write,
        })
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<FastSingleFieldSerializer<CountingWriter<WritePtr>>> {
        self.new_u64_fast_field_with_idx(field, min_value, max_value, 0)
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
        idx: usize,
    ) -> io::Result<FastSingleFieldSerializer<CountingWriter<WritePtr>>> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        FastSingleFieldSerializer::open(field_write, min_value, max_value)
    }

    /// Closes the serializer
    ///
    /// After this call the data must be persistently save on disk.
    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()
    }
}

pub struct FastSingleFieldSerializer<'a, W: Write + 'a> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
}

impl<'a, W: Write> FastSingleFieldSerializer<'a, W> {
    fn open(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<FastSingleFieldSerializer<'a, W>> {
        min_value.serialize(write)?;
        let amplitude = max_value - min_value;
        amplitude.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new(num_bits as usize);
        Ok(FastSingleFieldSerializer {
            write: write,
            bit_packer: bit_packer,
            min_value: min_value,
        })
    }

    /// Pushes a new value to the currently open u64 fast field.
    pub fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer.write(val_to_write, &mut self.write)?;
        Ok(())
    }

    pub fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)
    }
}
