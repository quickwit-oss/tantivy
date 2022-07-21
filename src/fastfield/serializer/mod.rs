mod gcd;

use std::io::{self, Write};

use common::{BinarySerializable, CountingWriter};
pub use fastfield_codecs::bitpacked::{
    BitpackedFastFieldSerializer, BitpackedFastFieldSerializerLegacy,
};
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldSerializer;
use fastfield_codecs::multilinearinterpol::MultiLinearInterpolFastFieldSerializer;
pub use fastfield_codecs::{FastFieldCodecSerializer, FastFieldDataAccess, FastFieldStats};

use crate::directory::{CompositeWrite, WritePtr};
use crate::fastfield::serializer::gcd::{find_gcd, GCD_DEFAULT};
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
    codec_enable_checker: FastFieldCodecEnableCheck,
}

pub struct FastFieldCodecEnableCheck {
    enabled_codecs: Vec<FastFieldCodecName>,
}
impl FastFieldCodecEnableCheck {
    fn allow_all() -> Self {
        FastFieldCodecEnableCheck {
            enabled_codecs: ALL_CODECS.to_vec(),
        }
    }
    fn is_enabled(&self, codec_name: FastFieldCodecName) -> bool {
        self.enabled_codecs.contains(&codec_name)
    }
}

impl From<FastFieldCodecName> for FastFieldCodecEnableCheck {
    fn from(codec_name: FastFieldCodecName) -> Self {
        FastFieldCodecEnableCheck {
            enabled_codecs: vec![codec_name],
        }
    }
}

pub const FF_HEADER_MAGIC_NUMBER: u8 = 123u8;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
enum FastFieldCodecName {
    Bitpacked,
    LinearInterpol,
    BlockwiseLinearInterpol,
}
const ALL_CODECS: &[FastFieldCodecName; 3] = &[
    FastFieldCodecName::Bitpacked,
    FastFieldCodecName::LinearInterpol,
    FastFieldCodecName::BlockwiseLinearInterpol,
];

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
        Self::from_write_with_codec(write, FastFieldCodecEnableCheck::allow_all())
    }

    /// Constructor
    pub fn from_write_with_codec(
        write: WritePtr,
        codec_enable_checker: FastFieldCodecEnableCheck,
    ) -> io::Result<CompositeFastFieldSerializer> {
        // just making room for the pointer to header.
        let composite_write = CompositeWrite::wrap(write);
        Ok(CompositeFastFieldSerializer {
            composite_write,
            codec_enable_checker,
        })
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field<F, I>(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        iter_gen: F,
    ) -> io::Result<()>
    where
        F: Fn() -> I,
        I: Iterator<Item = u64>,
    {
        self.create_auto_detect_u64_fast_field_with_idx(
            field,
            stats,
            fastfield_accessor,
            iter_gen,
            0,
        )
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn write_header<W: Write>(
        field_write: &mut W,
        codec_id: u8,
        stats: FastFieldStats,
        gcd: Option<u64>,
    ) -> io::Result<()> {
        FF_HEADER_MAGIC_NUMBER.serialize(field_write)?;
        let header_version = 1_u8;
        header_version.serialize(field_write)?;

        codec_id.serialize(field_write)?;
        gcd.unwrap_or(GCD_DEFAULT).serialize(field_write)?;
        stats.min_value.serialize(field_write)?;

        Ok(())
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field_with_idx<F, I>(
        &mut self,
        field: Field,
        stats: FastFieldStats,
        fastfield_accessor: impl FastFieldDataAccess,
        iter_gen: F,
        idx: usize,
    ) -> io::Result<()>
    where
        F: Fn() -> I,
        I: Iterator<Item = u64>,
    {
        let field_write = self.composite_write.for_field_with_idx(field, idx);

        struct WrappedFFAccess<T: FastFieldDataAccess> {
            fastfield_accessor: T,
            min_value: u64,
            gcd: u64,
        }
        impl<T: FastFieldDataAccess> FastFieldDataAccess for WrappedFFAccess<T> {
            fn get_val(&self, position: u64) -> u64 {
                (self.fastfield_accessor.get_val(position) - self.min_value) / self.gcd
            }
        }
        let gcd = find_gcd(&fastfield_accessor, stats.clone(), iter_gen()).unwrap_or(GCD_DEFAULT);
        let fastfield_accessor = WrappedFFAccess {
            fastfield_accessor,
            min_value: stats.min_value,
            gcd,
        };

        let mut estimations = vec![];

        if self
            .codec_enable_checker
            .is_enabled(FastFieldCodecName::Bitpacked)
        {
            codec_estimation::<BitpackedFastFieldSerializer, _>(
                stats.clone(),
                &fastfield_accessor,
                &mut estimations,
            );
        }
        if self
            .codec_enable_checker
            .is_enabled(FastFieldCodecName::LinearInterpol)
        {
            codec_estimation::<LinearInterpolFastFieldSerializer, _>(
                stats.clone(),
                &fastfield_accessor,
                &mut estimations,
            );
        }
        if self
            .codec_enable_checker
            .is_enabled(FastFieldCodecName::BlockwiseLinearInterpol)
        {
            codec_estimation::<MultiLinearInterpolFastFieldSerializer, _>(
                stats.clone(),
                &fastfield_accessor,
                &mut estimations,
            );
        }
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

        Self::write_header(field_write, id, stats.clone(), Some(gcd))?;
        let min_value = stats.min_value;
        // let min_value = 0;
        let stats = FastFieldStats {
            min_value: 0,
            max_value: (stats.max_value - stats.min_value) / gcd,
            num_vals: stats.num_vals,
        };
        let iter1 = iter_gen().map(|val| (val - min_value) / gcd);
        let iter2 = iter_gen().map(|val| (val - min_value) / gcd);
        // let iter1 = iter_gen();
        // let iter2 = iter_gen();
        match name {
            BitpackedFastFieldSerializer::NAME => {
                BitpackedFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    iter1,
                    iter2,
                )?;
            }
            LinearInterpolFastFieldSerializer::NAME => {
                LinearInterpolFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    iter1,
                    iter2,
                )?;
            }
            MultiLinearInterpolFastFieldSerializer::NAME => {
                MultiLinearInterpolFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
                    stats,
                    iter1,
                    iter2,
                )?;
            }
            _ => {
                panic!("unknown fastfield serializer {}", name)
            }
        }
        field_write.flush()?;

        Ok(())
    }

    /// Start serializing a new u64 fast field
    pub fn serialize_into(
        &mut self,
        field: Field,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializerLegacy<'_, CountingWriter<WritePtr>>> {
        self.new_u64_fast_field_with_idx(field, min_value, max_value, 0)
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
