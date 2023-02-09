use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use columnar::{
    BytesColumn, Column, ColumnType, ColumnValues, ColumnarReader, DynamicColumn,
    DynamicColumnHandle, HasAssociatedColumnType, StrColumn,
};

use crate::directory::FileSlice;
use crate::schema::Schema;
use crate::space_usage::{FieldUsage, PerFieldSpaceUsage};

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

    pub(crate) fn columnar(&self) -> &ColumnarReader {
        self.columnar.as_ref()
    }

    pub(crate) fn space_usage(&self, schema: &Schema) -> io::Result<PerFieldSpaceUsage> {
        let mut per_field_usages: Vec<FieldUsage> = Default::default();
        for (field, field_entry) in schema.fields() {
            let column_handles = self.columnar.read_columns(field_entry.name())?;
            let num_bytes: usize = column_handles
                .iter()
                .map(|column_handle| column_handle.num_bytes())
                .sum();
            let mut field_usage = FieldUsage::empty(field);
            field_usage.add_field_idx(0, num_bytes);
            per_field_usages.push(field_usage);
        }
        // TODO fix space usage for JSON fields.
        Ok(PerFieldSpaceUsage::new(per_field_usages))
    }

    /// Returns a typed column associated to a given field name.
    ///
    /// If no column associated with that field_name exists,
    /// or existing columns do not have the required type,
    /// returns `None`.
    pub fn column_opt<T>(&self, field_name: &str) -> crate::Result<Option<Column<T>>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + 'static,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let column_type = T::column_type();
        let Some(dynamic_column_handle) = self.dynamic_column_handle(field_name, column_type)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returns the number of `bytes` associated with a column.
    pub fn column_num_bytes(&self, field: &str) -> crate::Result<usize> {
        Ok(self
            .columnar
            .read_columns(field)?
            .into_iter()
            .map(|column_handle| column_handle.num_bytes())
            .sum())
    }

    /// Returns a typed column value object.
    ///
    /// In that column value:
    /// - Rows with no value are associated with the default value.
    /// - Rows with several values are associated with the first value.
    pub fn column_first_or_default<T>(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<T>>>
    where
        T: PartialOrd + Copy + HasAssociatedColumnType + Send + Sync + 'static,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let col_opt: Option<Column<T>> = self.column_opt(field)?;
        if let Some(col) = col_opt {
            Ok(col.first_or_default_col(T::default_value()))
        } else {
            Err(crate::TantivyError::SchemaError(format!(
                "Field `{field}` is missing or is not configured as a fast field."
            )))
        }
    }

    /// Returns the `u64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns an Error.
    pub fn u64(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<u64>>> {
        self.column_first_or_default(field)
    }

    /// Returns the `date` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a date fast field, this method returns an Error.
    pub fn date(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<columnar::DateTime>>> {
        self.column_first_or_default(field)
    }

    /// Returns the `ip` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u128 fast field, this method returns an Error.
    pub fn ip_addr(&self, field: &str) -> crate::Result<Arc<dyn ColumnValues<Ipv6Addr>>> {
        self.column_first_or_default(field)
    }

    /// Returns a `str` column.
    pub fn str(&self, field_name: &str) -> crate::Result<Option<StrColumn>> {
        let Some(dynamic_column_handle) = self.dynamic_column_handle(field_name, ColumnType::Str)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returns a `bytes` column.
    pub fn bytes(&self, field_name: &str) -> crate::Result<Option<BytesColumn>> {
        let Some(dynamic_column_handle) = self.dynamic_column_handle(field_name, ColumnType::Bytes)?
        else {
            return Ok(None);
        };
        let dynamic_column = dynamic_column_handle.open()?;
        Ok(dynamic_column.into())
    }

    /// Returning a `dynamic_column_handle`.
    pub fn dynamic_column_handle(
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

    /// Returns the `u64` column used to represent any `u64`-mapped typed (i64, u64, f64, DateTime).
    #[doc(hidden)]
    pub fn u64_lenient(&self, field_name: &str) -> crate::Result<Option<Column<u64>>> {
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
    pub fn i64(&self, field_name: &str) -> crate::Result<Arc<dyn ColumnValues<i64>>> {
        self.column_first_or_default(field_name)
    }

    /// Returns the `f64` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns an Error.
    pub fn f64(&self, field_name: &str) -> crate::Result<Arc<dyn ColumnValues<f64>>> {
        self.column_first_or_default(field_name)
    }

    /// Returns the `bool` fast field reader reader associated with `field`.
    ///
    /// If `field` is not a bool fast field, this method returns an Error.
    pub fn bool(&self, field_name: &str) -> crate::Result<Arc<dyn ColumnValues<bool>>> {
        self.column_first_or_default(field_name)
    }
}
