mod bitpacked;
mod linearinterpol;

use crate::common::BinarySerializable;
use crate::common::CompositeWrite;
use crate::common::CountingWriter;
use crate::directory::WritePtr;
use crate::schema::Field;
use crate::DocId;
pub use bitpacked::BitpackedFastFieldSerializer;
use std::io::{self, Write};

/// FastFieldReader is the trait to access fast field data.
pub trait FastFieldDataAccess: Clone {
    //type IteratorType: Iterator<Item = u64>;
    /// Return the value associated to the given document.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    fn get(&self, doc: DocId) -> u64;
}

/// `CompositeFastFieldSerializer` is in charge of serializing
/// fastfields on disk.
///
/// Fast fields have different encodings like bit-packing.
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

    /// Serialize data into a new u64 fast field. The compression will be detected automatically.
    pub fn create_auto_detect_u64_fast_field(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        data_iter_1: impl Iterator<Item = u64>,
        data_iter_2: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        let field_write = self.composite_write.for_field_with_idx(field, 0);

        let (_ratio, (name, id)) = (
            BitpackedFastFieldSerializer::<Vec<u8>>::estimate(&fastfield_accessor, stats.clone()),
            BitpackedFastFieldSerializer::<Vec<u8>>::codec_id(),
        );

        id.serialize(field_write)?;
        if name == BitpackedFastFieldSerializer::<Vec<u8>>::codec_id().0 {
            BitpackedFastFieldSerializer::create(
                field_write,
                &fastfield_accessor,
                stats,
                data_iter_1,
            )?;
        } else {
            panic!("unknown fastfield serializer {}", name);
        };

        Ok(())
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
        // Prepend codec id to field data for compatibility with DynamicFastFieldReader.
        let (_name, id) = BitpackedFastFieldSerializer::<Vec<u8>>::codec_id();
        id.serialize(field_write)?;
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

#[derive(Debug, Clone)]
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
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
        fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> (f32, &'static str);
    /// the unique (name, id) of the compressor. Used to distinguish when de/serializing.
    fn codec_id() -> (&'static str, u8);
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
