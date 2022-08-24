use std::io::{self, Write};
use std::num::NonZeroU64;

use common::{BinarySerializable, CountingWriter};
pub use fastfield_codecs::bitpacked::{
    BitpackedFastFieldSerializer, BitpackedFastFieldSerializerLegacy,
};
use fastfield_codecs::linearinterpol::LinearInterpolFastFieldSerializer;
use fastfield_codecs::multilinearinterpol::MultiLinearInterpolFastFieldSerializer;
pub use fastfield_codecs::{FastFieldCodecSerializer, FastFieldDataAccess, FastFieldStats};

use super::{find_gcd, FastFieldCodecName, ALL_CODECS, GCD_DEFAULT};
use crate::directory::{CompositeWrite, WritePtr};
use crate::fastfield::gcd::write_gcd_header;
use crate::fastfield::GCD_CODEC_ID;
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

#[derive(Debug, Clone)]
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

// use this, when this is merged and stabilized explicit_generic_args_with_impl_trait
// https://github.com/rust-lang/rust/pull/86176
fn codec_estimation<T: FastFieldCodecSerializer, A: FastFieldDataAccess>(
    fastfield_accessor: &A,
    estimations: &mut Vec<(f32, &str, u8)>,
) {
    if !T::is_applicable(fastfield_accessor) {
        return;
    }
    let (ratio, name, id) = (T::estimate(fastfield_accessor), T::NAME, T::ID);
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
    pub fn create_auto_detect_u64_fast_field(
        &mut self,
        field: Field,
        fastfield_accessor: impl FastFieldDataAccess,
    ) -> io::Result<()> {
        self.create_auto_detect_u64_fast_field_with_idx(field, fastfield_accessor, 0)
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn write_header<W: Write>(field_write: &mut W, codec_id: u8) -> io::Result<()> {
        codec_id.serialize(field_write)?;

        Ok(())
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field_with_idx(
        &mut self,
        field: Field,
        fastfield_accessor: impl FastFieldDataAccess,
        idx: usize,
    ) -> io::Result<()> {
        let min_value = fastfield_accessor.min_value();
        let field_write = self.composite_write.for_field_with_idx(field, idx);
        let gcd = find_gcd(fastfield_accessor.iter().map(|val| val - min_value))
            .map(NonZeroU64::get)
            .unwrap_or(GCD_DEFAULT);

        if gcd == 1 {
            return Self::create_auto_detect_u64_fast_field_with_idx_gcd(
                self.codec_enable_checker.clone(),
                field,
                field_write,
                fastfield_accessor,
            );
        }

        Self::write_header(field_write, GCD_CODEC_ID)?;
        struct GCDWrappedFFAccess<T: FastFieldDataAccess> {
            fastfield_accessor: T,
            base_value: u64,
            max_value: u64,
            num_vals: u64,
            gcd: u64,
        }
        impl<T: FastFieldDataAccess> FastFieldDataAccess for GCDWrappedFFAccess<T> {
            fn get_val(&self, position: u64) -> u64 {
                (self.fastfield_accessor.get_val(position) - self.base_value) / self.gcd
            }
            fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = u64> + 'b> {
                Box::new(
                    self.fastfield_accessor
                        .iter()
                        .map(|val| (val - self.base_value) / self.gcd),
                )
            }
            fn min_value(&self) -> u64 {
                0
            }

            fn max_value(&self) -> u64 {
                self.max_value
            }

            fn num_vals(&self) -> u64 {
                self.num_vals
            }
        }

        let num_vals = fastfield_accessor.num_vals();
        let base_value = fastfield_accessor.min_value();
        let max_value = (fastfield_accessor.max_value() - fastfield_accessor.min_value()) / gcd;

        let fastfield_accessor = GCDWrappedFFAccess {
            fastfield_accessor,
            base_value,
            max_value,
            num_vals,
            gcd,
        };

        Self::create_auto_detect_u64_fast_field_with_idx_gcd(
            self.codec_enable_checker.clone(),
            field,
            field_write,
            fastfield_accessor,
        )?;
        write_gcd_header(field_write, base_value, gcd)?;
        Ok(())
    }

    /// Serialize data into a new u64 fast field. The best compression codec will be chosen
    /// automatically.
    pub fn create_auto_detect_u64_fast_field_with_idx_gcd<W: Write>(
        codec_enable_checker: FastFieldCodecEnableCheck,
        field: Field,
        field_write: &mut CountingWriter<W>,
        fastfield_accessor: impl FastFieldDataAccess,
    ) -> io::Result<()> {
        let mut estimations = vec![];

        if codec_enable_checker.is_enabled(FastFieldCodecName::Bitpacked) {
            codec_estimation::<BitpackedFastFieldSerializer, _>(
                &fastfield_accessor,
                &mut estimations,
            );
        }
        if codec_enable_checker.is_enabled(FastFieldCodecName::LinearInterpol) {
            codec_estimation::<LinearInterpolFastFieldSerializer, _>(
                &fastfield_accessor,
                &mut estimations,
            );
        }
        if codec_enable_checker.is_enabled(FastFieldCodecName::BlockwiseLinearInterpol) {
            codec_estimation::<MultiLinearInterpolFastFieldSerializer, _>(
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

        Self::write_header(field_write, id)?;
        match name {
            BitpackedFastFieldSerializer::NAME => {
                BitpackedFastFieldSerializer::serialize(field_write, &fastfield_accessor)?;
            }
            LinearInterpolFastFieldSerializer::NAME => {
                LinearInterpolFastFieldSerializer::serialize(field_write, &fastfield_accessor)?;
            }
            MultiLinearInterpolFastFieldSerializer::NAME => {
                MultiLinearInterpolFastFieldSerializer::serialize(
                    field_write,
                    &fastfield_accessor,
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
