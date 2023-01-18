use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::{
    ColumnType, ColumnValues, ColumnarReader, DynamicColumn, DynamicColumnHandle,
    HasAssociatedColumnType, NumericalType,
};
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
    columnar: Arc<ColumnarReader>,
}

impl FastFieldReaders {
    pub(crate) fn open(fast_field_file: FileSlice) -> io::Result<FastFieldReaders> {
        let columnar = Arc::new(ColumnarReader::open(fast_field_file)?);
        Ok(FastFieldReaders { columnar })
    }

    pub(crate) fn space_usage(&self) -> PerFieldSpaceUsage {
        todo!()
    }

    pub fn typed_column_opt<T>(
        &self,
        field_name: &str,
    ) -> crate::Result<Option<columnar::Column<T>>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + Default + 'static,
        DynamicColumn: Into<Option<columnar::Column<T>>>,
    {
        let column_type = T::column_type();
        let Some(dynamic_column_handle) = self.column_handle(field_name, column_type)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    pub fn column_num_bytes(&self, field: &str) -> crate::Result<usize> {
        Ok(self
            .columnar
            .read_columns(field)?
            .into_iter()
            .map(|column_handle| column_handle.num_bytes())
            .sum())
    }

    pub fn typed_column_first_or_default<T>(&self, field: &str) -> crate::Result<Arc<dyn Column<T>>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + Default + 'static,
        DynamicColumn: Into<Option<columnar::Column<T>>>,
    {
        let col_opt = self.typed_column_opt(field)?;
        if let Some(col) = col_opt {
            Ok(col.first_or_default_col(T::default()))
        } else {
            todo!();
        }
    }

    /// Returns the `u64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<u64>>> {
        self.typed_column_first_or_default(field)
    }

    /// Returns the `date` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a date fast field, this method returns an Error.
    pub fn date(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<columnar::DateTime>>> {
        self.typed_column_first_or_default(field)
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

    pub fn column_handle(
        &self,
        field_name: &str,
        column_type: ColumnType,
    ) -> crate::Result<Option<DynamicColumnHandle>> {
        let dynamic_column_handle_opt = self
            .columnar
            .read_columns(field_name)?
            .into_iter()
            .filter(|column| column.column_type() == column_type)
            .next();
        Ok(dynamic_column_handle_opt)
    }

    pub fn u64_lenient(&self, field_name: &str) -> crate::Result<Option<columnar::Column<u64>>> {
        for col in self.columnar.read_columns(field_name)? {
            if let Some(col_u64) = col.open_u64_lenient()? {
                return Ok(Some(col_u64));
            }
        }
        Ok(None)
    }

    /// Returns the `i64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns an Error.
    pub fn i64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<i64>>> {
        self.typed_column_first_or_default(field_name)
    }

    /// Returns the `f64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field_name: &str) -> crate::Result<Arc<dyn Column<f64>>> {
        self.typed_column_first_or_default(field_name)
    }

    /// Returns the `bool` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool fast field, this method returns an Error.
    pub fn bool(&self, field_name: &str) -> crate::Result<Arc<dyn Column<bool>>> {
        self.typed_column_first_or_default(field_name)
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
