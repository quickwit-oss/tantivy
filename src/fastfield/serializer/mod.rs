use crate::directory::CompositeWrite;
use crate::directory::WritePtr;
use crate::schema::Field;
use common::BinarySerializable;
use common::CountingWriter;
pub use fastfield_codecs::bitpacked::BitpackedFastFieldSerializer;
pub use fastfield_codecs::bitpacked::BitpackedFastFieldSerializerLegacy;
use fastfield_codecs::piecewise_linear::PiecewiseLinearFastFieldSerializer;
pub use fastfield_codecs::FastFieldCodecSerializer;
pub use fastfield_codecs::FastFieldDataAccess;
pub use fastfield_codecs::FastFieldStats;
use itertools::Itertools;
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

#[derive(Debug)]
pub struct CodecEstimationResult<'a> {
    pub ratio: f32,
    pub name: &'a str,
    pub id: u8,
}

// TODO: use this when this is merged and stabilized explicit_generic_args_with_impl_trait
// https://github.com/rust-lang/rust/pull/86176
fn codec_estimation<T: FastFieldCodecSerializer, A: FastFieldDataAccess>(
    stats: FastFieldStats,
    fastfield_accessor: &A,
) -> CodecEstimationResult {
    if !T::is_applicable(fastfield_accessor, stats.clone()) {
        return CodecEstimationResult {
            ratio: f32::MAX,
            name: T::NAME,
            id: T::ID,
        };
    }
    return CodecEstimationResult {
        ratio: T::estimate_compression_ratio(fastfield_accessor, stats),
        name: T::NAME,
        id: T::ID,
    };
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
    pub fn new_u64_fast_field_with_best_codec(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        data_iter_1: impl Iterator<Item = u64>,
        data_iter_2: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        self.new_u64_fast_field_with_idx_with_best_codec(
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
    pub fn new_u64_fast_field_with_idx_with_best_codec(
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
        estimations.push(codec_estimation::<BitpackedFastFieldSerializer, _>(
            stats.clone(),
            &fastfield_accessor,
        ));
        estimations.push(codec_estimation::<PiecewiseLinearFastFieldSerializer, _>(
            stats.clone(),
            &fastfield_accessor,
        ));
        let best_codec_result = estimations
            .iter()
            .sorted_by(|result_a, result_b| {
                result_a
                    .ratio
                    .partial_cmp(&result_b.ratio)
                    .expect("Ratio cannot be nan.")
            })
            .next()
            .expect("A codec must be present.");
        debug!(
            "Choosing fast field codec {} for field_id {:?} among {:?}",
            best_codec_result.name, field, estimations,
        );
        best_codec_result.id.serialize(field_write)?;
        match best_codec_result.name {
            BitpackedFastFieldSerializer::NAME => {
                BitpackedFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                    data_iter_2,
                )?;
            }
            PiecewiseLinearFastFieldSerializer::NAME => {
                PiecewiseLinearFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    data_iter_1,
                    data_iter_2,
                )?;
            }
            _ => {
                panic!("unknown fastfield serializer {}", best_codec_result.name)
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

#[cfg(test)]
mod tests {
    use common::BinarySerializable;
    use std::path::Path;

    use fastfield_codecs::FastFieldStats;
    use itertools::Itertools;

    use crate::{
        directory::{RamDirectory, WritePtr},
        schema::Field,
        Directory,
    };

    use super::CompositeFastFieldSerializer;

    #[test]
    fn new_u64_fast_field_with_best_codec() -> crate::Result<()> {
        let directory: RamDirectory = RamDirectory::create();
        let path = Path::new("test");
        let write: WritePtr = directory.open_write(path)?;
        let mut serializer = CompositeFastFieldSerializer::from_write(write)?;
        let vals = (0..10000u64).into_iter().collect_vec();
        let stats = FastFieldStats {
            min_value: 0,
            max_value: 9999,
            num_vals: vals.len() as u64,
        };
        serializer.new_u64_fast_field_with_best_codec(
            Field::from_field_id(0),
            stats,
            vals.clone(),
            vals.clone().into_iter(),
            vals.into_iter(),
        )?;
        serializer.close()?;
        // get the codecs id
        let mut bytes = directory.open_read(path)?.read_bytes()?;
        let codec_id = u8::deserialize(&mut bytes)?;
        // Codec id = 4 is piecewise linear.
        assert_eq!(codec_id, 4);
        Ok(())
    }
}
