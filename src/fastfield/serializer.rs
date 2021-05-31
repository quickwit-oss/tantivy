use crate::common::BinarySerializable;
use crate::common::CompositeWrite;
use crate::common::CountingWriter;
use crate::directory::WritePtr;
use crate::schema::Field;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

use super::FastFieldReader;

/// `CompositeFastFieldSerializer` is in charge of serializing
/// fastfields on disk.
///
/// Fast fields have differnt encodings like bit-packing.
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
pub struct CompositeFastFieldSerializer {
    composite_write: CompositeWrite<WritePtr>,
}

impl CompositeFastFieldSerializer {
    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<CompositeFastFieldSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(CompositeFastFieldSerializer { composite_write })
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializer<'_, CountingWriter<WritePtr>>> {
        self.new_u64_fast_field_with_idx(field, min_value, max_value, 0)
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
        idx: usize,
    ) -> io::Result<BitpackedFastFieldSerializer<'_, CountingWriter<WritePtr>>> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        BitpackedFastFieldSerializer::open(field_write, min_value, max_value)
    }

    /// Start serializing a new [u8] fast field
    pub fn new_bytes_fast_field_with_idx(
        &mut self,
        field: Field,
        idx: usize,
    ) -> FastBytesFieldSerializer<'_, CountingWriter<WritePtr>> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        FastBytesFieldSerializer { write: field_write }
    }

    /// Closes the serializer
    ///
    /// After this call the data must be persistently save on disk.
    pub fn close(self) -> io::Result<()> {
        self.composite_write.close()
    }
}

pub struct EstimationStats {}
/// The FastFieldSerializer trait is the common interface
/// implemented by every fastfield serializer variant.
///
/// `DynamicFastFieldSerializer` is the enum wrapping all variants.
/// It is used to create an serializer instance.
pub trait FastFieldSerializer {
    /// add value to serializer
    fn add_val(&mut self, val: u64) -> io::Result<()>;
    /// returns an estimate of the compression ratio.
    fn estimate(fastfield_accessor: impl FastFieldReader<u64>, stats: EstimationStats) -> f32;
    /// finish serializing a field.
    fn close_field(self) -> io::Result<()>;
}

pub trait ValueAccessor {}

pub enum DynamicFastFieldSerializer<'a, W: Write> {
    Bitpacked(BitpackedFastFieldSerializer<'a, W>),
}

impl<'a, W: Write> DynamicFastFieldSerializer<'a, W> {
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn open(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<DynamicFastFieldSerializer<'a, W>> {
        assert!(min_value <= max_value);
        min_value.serialize(write)?;
        let amplitude = max_value - min_value;
        amplitude.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(DynamicFastFieldSerializer::Bitpacked(
            BitpackedFastFieldSerializer {
                bit_packer,
                write,
                min_value,
                num_bits,
            },
        ))
    }
}
impl<'a, W: Write> FastFieldSerializer for DynamicFastFieldSerializer<'a, W> {
    fn add_val(&mut self, val: u64) -> io::Result<()> {
        match self {
            Self::Bitpacked(serializer) => serializer.add_val(val),
        }
    }
    fn estimate(fastfield_accessor: impl FastFieldReader<u64>, stats: EstimationStats) -> f32 {
        unimplemented!()
        //match self {
        //Self::Bitpacked(serializer) => 1.0,
        //}
    }
    fn close_field(mut self) -> io::Result<()> {
        match self {
            Self::Bitpacked(serializer) => serializer.close_field(),
        }
    }
}

pub struct BitpackedFastFieldSerializer<'a, W: Write> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
    num_bits: u8,
}

impl<'a, W: Write> BitpackedFastFieldSerializer<'a, W> {
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    fn open(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializer<'a, W>> {
        assert!(min_value <= max_value);
        min_value.serialize(write)?;
        let amplitude = max_value - min_value;
        amplitude.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(BitpackedFastFieldSerializer {
            bit_packer,
            write,
            min_value,
            num_bits,
        })
    }
}

impl<'a, W: Write> FastFieldSerializer for BitpackedFastFieldSerializer<'a, W> {
    /// Pushes a new value to the currently open u64 fast field.
    fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer
            .write(val_to_write, self.num_bits, &mut self.write)?;
        Ok(())
    }
    fn estimate(fastfield_accessor: impl FastFieldReader<u64>, stats: EstimationStats) -> f32 {
        1.0
    }
    fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)
    }
}

pub struct FastBytesFieldSerializer<'a, W: Write> {
    write: &'a mut W,
}

impl<'a, W: Write> FastBytesFieldSerializer<'a, W> {
    pub fn write_all(&mut self, vals: &[u8]) -> io::Result<()> {
        self.write.write_all(vals)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.write.flush()
    }
}
