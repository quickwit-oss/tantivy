use crate::common::BinarySerializable;
use crate::common::CompositeWrite;
use crate::common::CountingWriter;
use crate::directory::WritePtr;
use crate::schema::Field;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

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
    ) -> io::Result<DynamicFastFieldSerializer<'_, CountingWriter<WritePtr>>> {
        self.new_u64_fast_field_with_idx(field, min_value, max_value, 0)
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
        idx: usize,
    ) -> io::Result<DynamicFastFieldSerializer<'_, CountingWriter<WritePtr>>> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        DynamicFastFieldSerializer::open(field_write, min_value, max_value)
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

#[derive(Debug, Clone)]
pub struct EstimationStats {
    min_value: u64,
    max_value: u64,
}
/// The FastFieldSerializer trait is the common interface
/// implemented by every fastfield serializer variant.
///
/// `DynamicFastFieldSerializer` is the enum wrapping all variants.
/// It is used to create an serializer instance.
pub trait FastFieldSerializer {
    /// add value to serializer
    fn add_val(&mut self, val: u64) -> io::Result<()>;
    /// finish serializing a field.
    fn close_field(self) -> io::Result<()>;
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldSerializerEstimate {
    /// returns an estimate of the compression ratio.
    fn estimate(
        /*fastfield_accessor: impl FastFieldReader<u64>,*/ stats: EstimationStats,
    ) -> (f32, &'static str);
    /// the unique name of the compressor
    fn name() -> &'static str;
}

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
        let stats = EstimationStats {
            min_value,
            max_value,
        };
        let (_ratio, name) = (
            BitpackedFastFieldSerializer::<Vec<u8>>::estimate(stats),
            BitpackedFastFieldSerializer::<Vec<u8>>::name(),
        );
        Self::open_from_name(write, min_value, max_value, name)
    }

    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn open_from_name(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
        name: &str,
    ) -> io::Result<DynamicFastFieldSerializer<'a, W>> {
        // Weirdly the W generic on BitpackedFastFieldSerializer needs to be set,
        // although name() doesn't use it
        let variant = if name == BitpackedFastFieldSerializer::<Vec<u8>>::name() {
            DynamicFastFieldSerializer::Bitpacked(BitpackedFastFieldSerializer::open(
                write, min_value, max_value,
            )?)
        } else {
            panic!("unknown fastfield serializer {}", name);
        };

        Ok(variant)
    }
}
impl<'a, W: Write> FastFieldSerializer for DynamicFastFieldSerializer<'a, W> {
    fn add_val(&mut self, val: u64) -> io::Result<()> {
        match self {
            Self::Bitpacked(serializer) => serializer.add_val(val),
        }
    }
    fn close_field(self) -> io::Result<()> {
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

impl<'a, W: 'a + Write> FastFieldSerializer for BitpackedFastFieldSerializer<'a, W> {
    /// Pushes a new value to the currently open u64 fast field.
    fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer
            .write(val_to_write, self.num_bits, &mut self.write)?;
        Ok(())
    }
    fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)
    }
}

impl<'a, W: 'a + Write> FastFieldSerializerEstimate for BitpackedFastFieldSerializer<'a, W> {
    fn estimate(
        /*_fastfield_accessor: impl FastFieldReader<u64>, */ stats: EstimationStats,
    ) -> (f32, &'static str) {
        let amplitude = stats.max_value - stats.min_value;
        let num_bits = compute_num_bits(amplitude);
        let num_bits_uncompressed = 64;
        let ratio = num_bits as f32 / num_bits_uncompressed as f32;
        let name = Self::name();
        (ratio, name)
    }
    fn name() -> &'static str {
        "Bitpacked"
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
