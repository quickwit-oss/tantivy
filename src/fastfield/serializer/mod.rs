use crate::common::BinarySerializable;
use crate::common::CompositeWrite;
use crate::common::CountingWriter;
use crate::directory::WritePtr;
use crate::schema::Field;
use fastfield_codecs::CodecId;
//pub use bitpacked::BitpackedFastFieldSerializer;
pub use fastfield_codecs::bitpacked::BitpackedFastFieldSerializer;
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldSerializer;
pub use fastfield_codecs::FastFieldDataAccess;
pub use fastfield_codecs::FastFieldSerializerEstimate;
pub use fastfield_codecs::FastFieldStats;
use std::io::{self, Write};

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

        let mut estimations = vec![];

        {
            let (ratio, name, id) = (
                BitpackedFastFieldSerializer::<Vec<u8>>::estimate(
                    &fastfield_accessor,
                    stats.clone(),
                ),
                BitpackedFastFieldSerializer::<Vec<u8>>::NAME,
                BitpackedFastFieldSerializer::<Vec<u8>>::ID,
            );
            estimations.push((ratio, name, id));
        }
        {
            let (ratio, name, id) = (
                LinearInterpolFastFieldSerializer::estimate(&fastfield_accessor, stats.clone()),
                LinearInterpolFastFieldSerializer::NAME,
                LinearInterpolFastFieldSerializer::ID,
            );
            estimations.push((ratio, name, id));
        }
        estimations.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let (_ratio, name, id) = estimations[0];
        //estimations.sort_by_key(|el| el.0);
        id.serialize(field_write)?;
        match name {
            BitpackedFastFieldSerializer::<Vec<u8>>::NAME => {
                BitpackedFastFieldSerializer::create(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                )?;
            }
            LinearInterpolFastFieldSerializer::NAME => {
                LinearInterpolFastFieldSerializer::create(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                    data_iter_2,
                )?;
            }
            _ => {
                panic!("unknown fastfield serializer {}", name)
            }
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
        let id = BitpackedFastFieldSerializer::<Vec<u8>>::ID;
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
