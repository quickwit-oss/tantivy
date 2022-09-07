use std::io::{self, Write};

use common::CountingWriter;
pub use fastfield_codecs::{Column, FastFieldStats};
use fastfield_codecs::{FastFieldCodecType, MonotonicallyMappableToU64, ALL_CODEC_TYPES};

use crate::directory::{CompositeWrite, WritePtr};
use crate::schema::Field;

/// `CompositeFastFieldSerializer` is in charge of serializing
/// fastfields on disk.
///
/// Fast fields have different encodings like bit-packing.
///
/// `FastFieldWriter`s are in charge of pushing the data to
/// the serializer.
/// The serializer expects to receive the following calls.
///
/// * `create_auto_detect_u64_fast_field(...)`
/// * `create_auto_detect_u64_fast_field(...)`
/// * ...
/// * `let bytes_fastfield = new_bytes_fast_field(...)`
/// * `bytes_fastfield.write_all(...)`
/// * `bytes_fastfield.write_all(...)`
/// * `bytes_fastfield.flush()`
/// * ...
/// * `close()`
pub struct CompositeFastFieldSerializer {
    composite_write: CompositeWrite<WritePtr>,
    codec_types: Vec<FastFieldCodecType>,
}

impl CompositeFastFieldSerializer {
    /// New fast field serializer with all codec types
    pub fn from_write(write: WritePtr) -> io::Result<CompositeFastFieldSerializer> {
        Self::from_write_with_codec(write, &ALL_CODEC_TYPES)
    }

    /// New fast field serializer with allowed codec types
    pub fn from_write_with_codec(
        write: WritePtr,
        codec_types: &[FastFieldCodecType],
    ) -> io::Result<CompositeFastFieldSerializer> {
        let composite_write = CompositeWrite::wrap(write);
        Ok(CompositeFastFieldSerializer {
            composite_write,
            codec_types: codec_types.to_vec(),
        })
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field<T: MonotonicallyMappableToU64>(
        &mut self,
        field: Field,
        fastfield_accessor: impl Column<T>,
    ) -> io::Result<()> {
        self.create_auto_detect_u64_fast_field_with_idx(field, fastfield_accessor, 0)
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field_with_idx<T: MonotonicallyMappableToU64>(
        &mut self,
        field: Field,
        fastfield_accessor: impl Column<T>,
        idx: usize,
    ) -> io::Result<()> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        fastfield_codecs::serialize(fastfield_accessor, field_write, &self.codec_types)?;
        Ok(())
    }

    /// Start serializing a new [u8] fast field.
    ///
    /// The bytes will be stored as is, no compression will be applied.
    pub fn new_bytes_fast_field(
        &mut self,
        field: Field,
    ) -> FastBytesFieldSerializer<'_, CountingWriter<WritePtr>> {
        let field_write = self.composite_write.for_field_with_idx(field, 1);
        FastBytesFieldSerializer { write: field_write }
    }

    /// Closes the serializer
    ///
    /// After this call the data must be persistently saved on disk.
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
