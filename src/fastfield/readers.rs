use std::net::Ipv6Addr;
use std::sync::Arc;

use fastfield_codecs::{open, open_u128, Column};

use crate::directory::{CompositeFile, FileSlice};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::schema::{Field, FieldType, Schema};
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

pub(crate) fn type_and_cardinality(field_type: &FieldType) -> Option<FastType> {
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
        todo!();
    }

    /// Returns the `u64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field: &str) -> crate::Result<Arc<dyn Column<u64>>> {
        todo!();
    }

    /// Returns the `ip` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub fn ip_addr(&self, field: &str) -> crate::Result<Arc<dyn Column<Ipv6Addr>>> {
        todo!();
        // self.check_type(field, FastType::U128)?;
        // let bytes = self.fast_field_data(field, 0)?.read_bytes()?;
        // Ok(open_u128::<Ipv6Addr>(bytes)?)
    }

    /// Returns the `u128` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub(crate) fn u128(&self, field: &str) -> crate::Result<Arc<dyn Column<u128>>> {
        todo!();
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
        todo!()
    }

    /// Returns the `date` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a date fast field, this method returns an Error.
    pub fn date(&self, field_name: &str) -> crate::Result<Arc<dyn Column<DateTime>>> {
        todo!()
    }

    /// Returns the `f64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<f64>>> {
        todo!();
    }

    /// Returns the `bool` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool fast field, this method returns an Error.
    pub fn bool(&self, field_name: &str) -> crate::Result<Arc<dyn Column<bool>>> {
        todo!()
    }

    // Returns the `bytes` fast field reader associated with `field`.
    //
    // If `field` is not a bytes fast field, returns an Error.
    // pub fn bytes(&self, field: Field) -> crate::Result<BytesFastFieldReader> {
        // let field_entry = self.schema.get_field_entry(field);
        // if let FieldType::Bytes(bytes_option) = field_entry.field_type() {
        //     if !bytes_option.is_fast() {
        //         return Err(crate::TantivyError::SchemaError(format!(
        //             "Field {:?} is not a fast field.",
        //             field_entry.name()
        //         )));
        //     }
        //     let fast_field_idx_file = self.fast_field_data(field, 0)?;
        //     let fast_field_idx_bytes = fast_field_idx_file.read_bytes()?;
        //     let idx_reader = open(fast_field_idx_bytes)?;
        //     let data = self.fast_field_data(field, 1)?;
        //     BytesFastFieldReader::open(idx_reader, data)
        // } else {
        //     Err(FastFieldNotAvailableError::new(field_entry).into())
        // }
    // }
}
