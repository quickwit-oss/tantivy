use std::io::{self, Write};

use common::{BinarySerializable, CountingWriter};
pub use fastfield_codecs::bitpacked::{
    BitpackedFastFieldSerializer, BitpackedFastFieldSerializerLegacy,
};
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldSerializer;
use fastfield_codecs::multilinearinterpol::MultiLinearInterpolFastFieldSerializer;
pub use fastfield_codecs::{FastFieldCodecSerializer, FastFieldDataAccess, FastFieldStats};

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

// use this, when this is merged and stabilized explicit_generic_args_with_impl_trait
// https://github.com/rust-lang/rust/pull/86176
fn codec_estimation<T: FastFieldCodecSerializer, A: FastFieldDataAccess>(
    stats: FastFieldStats,
    fastfield_accessor: &A,
    estimations: &mut Vec<(f32, &str, u8)>,
) {
    if !T::is_applicable(fastfield_accessor, stats.clone()) {
        return;
    }
    let (ratio, name, id) = (T::estimate(fastfield_accessor, stats), T::NAME, T::ID);
    estimations.push((ratio, name, id));
}

impl CompositeFastFieldSerializer {
    /// Constructor
    pub fn from_write(write: WritePtr) -> io::Result<CompositeFastFieldSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(CompositeFastFieldSerializer { composite_write })
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        data_iter_1: impl Iterator<Item = u64>,
        data_iter_2: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        self.create_auto_detect_u64_fast_field_with_idx(
            field,
            stats,
            fastfield_accessor,
            data_iter_1,
            data_iter_2,
            0,
        )
    }
    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        data_iter_1: impl Iterator<Item = u64>,
        data_iter_2: impl Iterator<Item = u64>,
        idx: usize,
    ) -> io::Result<()> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);

        let mut estimations = vec![];

        codec_estimation::<BitpackedFastFieldSerializer, _>(
            stats.clone(),
            &fastfield_accessor,
            &mut estimations,
        );
        codec_estimation::<LinearInterpolFastFieldSerializer, _>(
            stats.clone(),
            &fastfield_accessor,
            &mut estimations,
        );
        codec_estimation::<MultiLinearInterpolFastFieldSerializer, _>(
            stats.clone(),
            &fastfield_accessor,
            &mut estimations,
        );
        if let Some(broken_estimation) = estimations.iter().find(|estimation| estimation.0.is_nan())
        {
            warn!(
                "broken estimation for fast field codec {}",
                broken_estimation.1
            );
        }
        // removing nan values for codecs with broken calculations, and max values which disables
        // codecs
        estimations.retain(|estimation| !estimation.0.is_nan() && estimation.0 != f32::MAX);
        estimations.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let (_ratio, name, id) = estimations[0];
        debug!(
            "choosing fast field codec {} for field_id {:?}",
            name, field
        ); // todo print actual field name
        id.serialize(field_write)?;
        match name {
            BitpackedFastFieldSerializer::NAME => {
                BitpackedFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                    data_iter_2,
                )?;
            }
            LinearInterpolFastFieldSerializer::NAME => {
                LinearInterpolFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                    data_iter_2,
                )?;
            }
            MultiLinearInterpolFastFieldSerializer::NAME => {
                MultiLinearInterpolFastFieldSerializer::serialize(
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
        field_write.flush()?;

        Ok(())
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializerLegacy<'_, CountingWriter<WritePtr>>> {
        self.new_u64_fast_field_with_idx(field, min_value, max_value, 0)
    }

    /// Start serializing a new u64 fast field
    pub fn new_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
        idx: usize,
    ) -> io::Result<BitpackedFastFieldSerializerLegacy<'_, CountingWriter<WritePtr>>> {
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        // Prepend codec id to field data for compatibility with DynamicFastFieldReader.
        let id = BitpackedFastFieldSerializer::ID;
        id.serialize(field_write)?;
        BitpackedFastFieldSerializerLegacy::open(field_write, min_value, max_value)
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
