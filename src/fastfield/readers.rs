use std::net::Ipv6Addr;
use std::sync::Arc;

use fastfield_codecs::{open, open_u128, Column};

use super::multivalued::MultiValuedFastFieldReader;
use crate::directory::{CompositeFile, FileSlice};
use crate::fastfield::{BytesFastFieldReader, FastFieldNotAvailableError, FastValue};
use crate::schema::{Cardinality, Field, FieldType, Schema};
use crate::space_usage::PerFieldSpaceUsage;
use crate::{DateTime, TantivyError};

/// Provides access to all of the BitpackedFastFieldReader.
///
/// Internally, `FastFieldReaders` have preloaded fast field readers,
/// and just wraps several `HashMap`.
#[derive(Clone)]
pub struct FastFieldReaders {
    schema: Schema,
    fast_fields_composite: CompositeFile,
}
#[derive(Eq, PartialEq, Debug)]
pub(crate) enum FastType {
    I64,
    U64,
    U128,
    F64,
    Bool,
    Date,
}

pub(crate) fn type_and_cardinality(field_type: &FieldType) -> Option<(FastType, Cardinality)> {
    todo!();
    // match field_type {
    //     FieldType::U64(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::U64, cardinality)),
    //     FieldType::I64(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::I64, cardinality)),
    //     FieldType::F64(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::F64, cardinality)),
    //     FieldType::Bool(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::Bool, cardinality)),
    //     FieldType::Date(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::Date, cardinality)),
    //     FieldType::Facet(_) => Some((FastType::U64, Cardinality::MultiValues)),
    //     FieldType::Str(options) if options.is_fast() => {
    //         Some((FastType::U64, Cardinality::MultiValues))
    //     }
    //     FieldType::IpAddr(options) => options
    //         .get_fastfield_cardinality()
    //         .map(|cardinality| (FastType::U128, cardinality)),
    //     _ => None,
    // }
}

impl FastFieldReaders {
    pub(crate) fn new(schema: Schema, fast_fields_composite: CompositeFile) -> FastFieldReaders {
        FastFieldReaders {
            schema,
            fast_fields_composite,
        }
    }

    pub(crate) fn space_usage(&self) -> PerFieldSpaceUsage {
        self.fast_fields_composite.space_usage()
    }

    #[doc(hidden)]
    pub fn fast_field_data(&self, field: Field, idx: usize) -> crate::Result<FileSlice> {
        self.fast_fields_composite
            .open_read_with_idx(field, idx)
            .ok_or_else(|| {
                let field_name = self.schema.get_field_entry(field).name();
                TantivyError::SchemaError(format!("Field({}) data was not found", field_name))
            })
    }

    fn check_type(
        &self,
        field: Field,
        expected_fast_type: FastType,
        expected_cardinality: Cardinality,
    ) -> crate::Result<()> {
        let field_entry = self.schema.get_field_entry(field);
        let (fast_type, cardinality) =
            type_and_cardinality(field_entry.field_type()).ok_or_else(|| {
                crate::TantivyError::SchemaError(format!(
                    "Field {:?} is not a fast field.",
                    field_entry.name()
                ))
            })?;
        if fast_type != expected_fast_type {
            return Err(crate::TantivyError::SchemaError(format!(
                "Field {:?} is of type {:?}, expected {:?}.",
                field_entry.name(),
                fast_type,
                expected_fast_type
            )));
        }
        if cardinality != expected_cardinality {
            return Err(crate::TantivyError::SchemaError(format!(
                "Field {:?} is of cardinality {:?}, expected {:?}.",
                field_entry.name(),
                cardinality,
                expected_cardinality
            )));
        }
        Ok(())
    }

    pub(crate) fn typed_fast_field_reader_with_idx<TFastValue: FastValue>(
        &self,
        field_name: &str,
        index: usize,
    ) -> crate::Result<Arc<dyn Column<TFastValue>>> {
        let field = self.schema.get_field(field_name)?;

        let fast_field_slice = self.fast_field_data(field, index)?;
        let bytes = fast_field_slice.read_bytes()?;
        let column = fastfield_codecs::open(bytes)?;
        Ok(column)
    }

    pub(crate) fn typed_fast_field_reader<TFastValue: FastValue>(
        &self,
        field_name: &str,
    ) -> crate::Result<Arc<dyn Column<TFastValue>>> {
        self.typed_fast_field_reader_with_idx(field_name, 0)
    }

    pub(crate) fn typed_fast_field_multi_reader<TFastValue: FastValue>(
        &self,
        field_name: &str,
    ) -> crate::Result<MultiValuedFastFieldReader<TFastValue>> {
        let idx_reader = self.typed_fast_field_reader(field_name)?;
        let vals_reader = self.typed_fast_field_reader_with_idx(field_name, 1)?;
        Ok(MultiValuedFastFieldReader::open(idx_reader, vals_reader))
    }

    /// Returns the `u64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<u64>>> {
        self.check_type(
            self.schema.get_field(field_name)?,
            FastType::U64,
            Cardinality::SingleValue,
        )?;
        self.typed_fast_field_reader(field_name)
    }

    /// Returns the `ip` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub fn ip_addr(&self, field_name: &str) -> crate::Result<Arc<dyn Column<Ipv6Addr>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::U128, Cardinality::SingleValue)?;
        let bytes = self.fast_field_data(field, 0)?.read_bytes()?;
        Ok(open_u128::<Ipv6Addr>(bytes)?)
    }

    /// Returns the `ip` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub fn ip_addrs(
        &self,
        field_name: &str,
    ) -> crate::Result<MultiValuedFastFieldReader<Ipv6Addr>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::U128, Cardinality::MultiValues)?;
        let idx_reader: Arc<dyn Column<u64>> = self.typed_fast_field_reader(field_name)?;

        let bytes = self.fast_field_data(field, 1)?.read_bytes()?;
        let vals_reader = open_u128::<Ipv6Addr>(bytes)?;

        Ok(MultiValuedFastFieldReader::open(idx_reader, vals_reader))
    }

    /// Returns the `u128` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub(crate) fn u128(&self, field_name: &str) -> crate::Result<Arc<dyn Column<u128>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::U128, Cardinality::SingleValue)?;
        let bytes = self.fast_field_data(field, 0)?.read_bytes()?;
        Ok(open_u128::<u128>(bytes)?)
    }

    /// Returns the `u128` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 multi-valued fast field, this method returns an Error.
    pub fn u128s(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<u128>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::U128, Cardinality::MultiValues)?;
        let idx_reader: Arc<dyn Column<u64>> =
            self.typed_fast_field_reader(self.schema.get_field_name(field))?;

        let bytes = self.fast_field_data(field, 1)?.read_bytes()?;
        let vals_reader = open_u128::<u128>(bytes)?;

        Ok(MultiValuedFastFieldReader::open(idx_reader, vals_reader))
    }

    /// Returns the `u64` fast field reader reader associated with `field`, regardless of whether
    /// the given field is effectively of type `u64` or not.
    ///
    /// If not, the fastfield reader will returns the u64-value associated with the original
    /// FastValue.
    pub fn u64_lenient(&self, field_name: &str) -> crate::Result<Arc<dyn Column<u64>>> {
        self.typed_fast_field_reader(field_name)
    }

    /// Returns the `i64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns an Error.
    pub fn i64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<i64>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::I64, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(self.schema.get_field_name(field))
    }

    /// Returns the `date` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a date fast field, this method returns an Error.
    pub fn date(&self, field_name: &str) -> crate::Result<Arc<dyn Column<DateTime>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::Date, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field_name)
    }

    /// Returns the `f64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<f64>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::F64, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field_name)
    }

    /// Returns the `bool` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool fast field, this method returns an Error.
    pub fn bool(&self, field_name: &str) -> crate::Result<Arc<dyn Column<bool>>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::Bool, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field_name)
    }

    /// Returns a `u64s` multi-valued fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 multi-valued fast field, this method returns an Error.
    pub fn u64s(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<u64>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::U64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(field_name)
    }

    /// Returns a `u64s` multi-valued fast field reader reader associated with `field`, regardless
    /// of whether the given field is effectively of type `u64` or not.
    ///
    /// If `field` is not a u64 multi-valued fast field, this method returns an Error.
    pub fn u64s_lenient(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<u64>> {
        self.typed_fast_field_multi_reader(field_name)
    }

    /// Returns a `i64s` multi-valued fast field reader reader associated with `field`.
    ///
    /// If `field` is not a i64 multi-valued fast field, this method returns an Error.
    pub fn i64s(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<i64>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::I64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(self.schema.get_field_name(field))
    }

    /// Returns a `f64s` multi-valued fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 multi-valued fast field, this method returns an Error.
    pub fn f64s(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<f64>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::F64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(self.schema.get_field_name(field))
    }

    /// Returns a `bools` multi-valued fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool multi-valued fast field, this method returns an Error.
    pub fn bools(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<bool>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::Bool, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(self.schema.get_field_name(field))
    }

    /// Returns a `time::OffsetDateTime` multi-valued fast field reader reader associated with
    /// `field`.
    ///
    /// If `field` is not a `time::OffsetDateTime` multi-valued fast field, this method returns an
    /// Error.
    pub fn dates(&self, field_name: &str) -> crate::Result<MultiValuedFastFieldReader<DateTime>> {
        let field = self.schema.get_field(field_name)?;
        self.check_type(field, FastType::Date, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(self.schema.get_field_name(field))
    }

    /// Returns the `bytes` fast field reader associated with `field`.
    ///
    /// If `field` is not a bytes fast field, returns an Error.
    pub fn bytes(&self, field_name: &str) -> crate::Result<BytesFastFieldReader> {
        let field = self.schema.get_field(field_name)?;
        let field_entry = self.schema.get_field_entry(field);
        if let FieldType::Bytes(bytes_option) = field_entry.field_type() {
            if !bytes_option.is_fast() {
                return Err(crate::TantivyError::SchemaError(format!(
                    "Field {:?} is not a fast field.",
                    field_entry.name()
                )));
            }
            let fast_field_idx_file = self.fast_field_data(field, 0)?;
            let fast_field_idx_bytes = fast_field_idx_file.read_bytes()?;
            let idx_reader = open(fast_field_idx_bytes)?;
            let data = self.fast_field_data(field, 1)?;
            BytesFastFieldReader::open(idx_reader, data)
        } else {
            Err(FastFieldNotAvailableError::new(field_entry).into())
        }
    }
}
