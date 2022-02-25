use super::reader::DynamicFastFieldReader;
use crate::directory::{CompositeFile, FileSlice};
use crate::fastfield::{
    BytesFastFieldReader, FastFieldNotAvailableError, FastValue, MultiValuedFastFieldReader,
};
use crate::schema::{Cardinality, Field, FieldType, Schema};
use crate::space_usage::PerFieldSpaceUsage;
use crate::TantivyError;

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
    F64,
    Date,
}

pub(crate) fn type_and_cardinality(field_type: &FieldType) -> Option<(FastType, Cardinality)> {
    match field_type {
        FieldType::U64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::U64, cardinality)),
        FieldType::I64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::I64, cardinality)),
        FieldType::F64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::F64, cardinality)),
        FieldType::Date(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::Date, cardinality)),
        FieldType::Facet(_) => Some((FastType::U64, Cardinality::MultiValues)),
        _ => None,
    }
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
        field: Field,
        index: usize,
    ) -> crate::Result<DynamicFastFieldReader<TFastValue>> {
        let fast_field_slice = self.fast_field_data(field, index)?;
        DynamicFastFieldReader::open(fast_field_slice)
    }
    pub(crate) fn typed_fast_field_reader<TFastValue: FastValue>(
        &self,
        field: Field,
    ) -> crate::Result<DynamicFastFieldReader<TFastValue>> {
        self.typed_fast_field_reader_with_idx(field, 0)
    }

    pub(crate) fn typed_fast_field_multi_reader<TFastValue: FastValue>(
        &self,
        field: Field,
    ) -> crate::Result<MultiValuedFastFieldReader<TFastValue>> {
        let idx_reader = self.typed_fast_field_reader(field)?;
        let vals_reader = self.typed_fast_field_reader_with_idx(field, 1)?;
        Ok(MultiValuedFastFieldReader::open(idx_reader, vals_reader))
    }

    /// Returns the `u64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field: Field) -> crate::Result<DynamicFastFieldReader<u64>> {
        self.check_type(field, FastType::U64, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field)
    }

    /// Returns the `u64` fast field reader reader associated to `field`, regardless of whether the
    /// given field is effectively of type `u64` or not.
    ///
    /// If not, the fastfield reader will returns the u64-value associated to the original
    /// FastValue.
    pub fn u64_lenient(&self, field: Field) -> crate::Result<DynamicFastFieldReader<u64>> {
        self.typed_fast_field_reader(field)
    }

    /// Returns the `i64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns an Error.
    pub fn i64(&self, field: Field) -> crate::Result<DynamicFastFieldReader<i64>> {
        self.check_type(field, FastType::I64, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field)
    }

    /// Returns the `i64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns an Error.
    pub fn date(&self, field: Field) -> crate::Result<DynamicFastFieldReader<crate::DateTime>> {
        self.check_type(field, FastType::Date, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field)
    }

    /// Returns the `f64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field: Field) -> crate::Result<DynamicFastFieldReader<f64>> {
        self.check_type(field, FastType::F64, Cardinality::SingleValue)?;
        self.typed_fast_field_reader(field)
    }

    /// Returns a `u64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u64 multi-valued fast field, this method returns an Error.
    pub fn u64s(&self, field: Field) -> crate::Result<MultiValuedFastFieldReader<u64>> {
        self.check_type(field, FastType::U64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(field)
    }

    /// Returns a `u64s` multi-valued fast field reader reader associated to `field`, regardless of
    /// whether the given field is effectively of type `u64` or not.
    ///
    /// If `field` is not a u64 multi-valued fast field, this method returns an Error.
    pub fn u64s_lenient(&self, field: Field) -> crate::Result<MultiValuedFastFieldReader<u64>> {
        self.typed_fast_field_multi_reader(field)
    }

    /// Returns a `i64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a i64 multi-valued fast field, this method returns an Error.
    pub fn i64s(&self, field: Field) -> crate::Result<MultiValuedFastFieldReader<i64>> {
        self.check_type(field, FastType::I64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(field)
    }

    /// Returns a `f64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a f64 multi-valued fast field, this method returns an Error.
    pub fn f64s(&self, field: Field) -> crate::Result<MultiValuedFastFieldReader<f64>> {
        self.check_type(field, FastType::F64, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(field)
    }

    /// Returns a `crate::DateTime` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a `crate::DateTime` multi-valued fast field, this method returns an Error.
    pub fn dates(
        &self,
        field: Field,
    ) -> crate::Result<MultiValuedFastFieldReader<crate::DateTime>> {
        self.check_type(field, FastType::Date, Cardinality::MultiValues)?;
        self.typed_fast_field_multi_reader(field)
    }

    /// Returns the `bytes` fast field reader associated to `field`.
    ///
    /// If `field` is not a bytes fast field, returns an Error.
    pub fn bytes(&self, field: Field) -> crate::Result<BytesFastFieldReader> {
        let field_entry = self.schema.get_field_entry(field);
        if let FieldType::Bytes(bytes_option) = field_entry.field_type() {
            if !bytes_option.is_fast() {
                return Err(crate::TantivyError::SchemaError(format!(
                    "Field {:?} is not a fast field.",
                    field_entry.name()
                )));
            }
            let fast_field_idx_file = self.fast_field_data(field, 0)?;
            let idx_reader = DynamicFastFieldReader::open(fast_field_idx_file)?;
            let data = self.fast_field_data(field, 1)?;
            BytesFastFieldReader::open(idx_reader, data)
        } else {
            Err(FastFieldNotAvailableError::new(field_entry).into())
        }
    }
}
