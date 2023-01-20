use std::io;
use std::net::Ipv6Addr;

use common::file_slice::FileSlice;
use common::{HasLen, OwnedBytes};

use crate::column::{BytesColumn, Column, StrColumn};
use crate::columnar::ColumnType;

#[derive(Clone)]
pub enum DynamicColumn {
    Bool(Column<bool>),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    IpAddr(Column<Ipv6Addr>),
    DateTime(Column<crate::DateTime>),
    Bytes(BytesColumn),
    Str(StrColumn),
}

macro_rules! static_dynamic_conversions {
    ($typ:ty, $enum_name:ident) => {
        impl Into<Option<$typ>> for DynamicColumn {
            fn into(self) -> Option<$typ> {
                if let DynamicColumn::$enum_name(col) = self {
                    Some(col)
                } else {
                    None
                }
            }
        }

        impl From<$typ> for DynamicColumn {
            fn from(typed_column: $typ) -> Self {
                DynamicColumn::$enum_name(typed_column)
            }
        }
    };
}

static_dynamic_conversions!(Column<bool>, Bool);
static_dynamic_conversions!(Column<u64>, U64);
static_dynamic_conversions!(Column<i64>, I64);
static_dynamic_conversions!(Column<f64>, F64);
static_dynamic_conversions!(Column<crate::DateTime>, DateTime);
static_dynamic_conversions!(StrColumn, Str);
static_dynamic_conversions!(BytesColumn, Bytes);
static_dynamic_conversions!(Column<Ipv6Addr>, IpAddr);

#[derive(Clone)]
pub struct DynamicColumnHandle {
    pub(crate) file_slice: FileSlice,
    pub(crate) column_type: ColumnType,
}

impl DynamicColumnHandle {
    // TODO rename load
    pub fn open(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes()?;
        self.open_internal(column_bytes)
    }

    // TODO rename load_async
    pub async fn open_async(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes_async().await?;
        self.open_internal(column_bytes)
    }

    /// Returns the `u64` fast field reader reader associated with `fields` of types
    /// Str, u64, i64, f64, or datetime.
    ///
    /// If not, the fastfield reader will returns the u64-value associated with the original
    /// FastValue.
    pub fn open_u64_lenient(&self) -> io::Result<Option<Column<u64>>> {
        let column_bytes = self.file_slice.read_bytes()?;
        match self.column_type {
            ColumnType::Str | ColumnType::Bytes => {
                let column: BytesColumn = crate::column::open_column_bytes(column_bytes)?;
                Ok(Some(column.term_ord_column))
            }
            ColumnType::Bool => Ok(None),
            ColumnType::IpAddr => Ok(None),
            ColumnType::I64 | ColumnType::U64 | ColumnType::F64 | ColumnType::DateTime => {
                let column = crate::column::open_column_u64::<u64>(column_bytes)?;
                Ok(Some(column))
            }
        }
    }

    fn open_internal(&self, column_bytes: OwnedBytes) -> io::Result<DynamicColumn> {
        let dynamic_column: DynamicColumn = match self.column_type {
            ColumnType::Bytes => {
                crate::column::open_column_bytes::<BytesColumn>(column_bytes)?.into()
            }
            ColumnType::Str => crate::column::open_column_bytes::<StrColumn>(column_bytes)?.into(),
            ColumnType::I64 => crate::column::open_column_u64::<i64>(column_bytes)?.into(),
            ColumnType::U64 => crate::column::open_column_u64::<u64>(column_bytes)?.into(),
            ColumnType::F64 => crate::column::open_column_u64::<f64>(column_bytes)?.into(),
            ColumnType::Bool => crate::column::open_column_u64::<bool>(column_bytes)?.into(),
            ColumnType::IpAddr => crate::column::open_column_u128::<Ipv6Addr>(column_bytes)?.into(),
            ColumnType::DateTime => {
                crate::column::open_column_u64::<crate::DateTime>(column_bytes)?.into()
            }
        };
        Ok(dynamic_column)
    }

    pub fn num_bytes(&self) -> usize {
        self.file_slice.len()
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
}
