use std::io;
use std::net::Ipv6Addr;

use common::file_slice::FileSlice;
use common::{HasLen, OwnedBytes};

use crate::column::{BytesColumn, Column, StrColumn};
use crate::columnar::ColumnType;
use crate::DateTime;

#[derive(Clone)]
pub enum DynamicColumn {
    Bool(Column<bool>),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    IpAddr(Column<Ipv6Addr>),
    DateTime(Column<DateTime>),
    Bytes(BytesColumn),
    Str(StrColumn),
}

impl From<Column<i64>> for DynamicColumn {
    fn from(column_i64: Column<i64>) -> Self {
        DynamicColumn::I64(column_i64)
    }
}

impl From<Column<u64>> for DynamicColumn {
    fn from(column_u64: Column<u64>) -> Self {
        DynamicColumn::U64(column_u64)
    }
}

impl From<Column<f64>> for DynamicColumn {
    fn from(column_f64: Column<f64>) -> Self {
        DynamicColumn::F64(column_f64)
    }
}

impl From<Column<bool>> for DynamicColumn {
    fn from(bool_column: Column<bool>) -> Self {
        DynamicColumn::Bool(bool_column)
    }
}

impl From<BytesColumn> for DynamicColumn {
    fn from(dictionary_encoded_col: BytesColumn) -> Self {
        DynamicColumn::Bytes(dictionary_encoded_col)
    }
}

impl From<StrColumn> for DynamicColumn {
    fn from(dictionary_encoded_col: StrColumn) -> Self {
        DynamicColumn::Str(dictionary_encoded_col)
    }
}

impl From<Column<Ipv6Addr>> for DynamicColumn {
    fn from(column: Column<Ipv6Addr>) -> Self {
        DynamicColumn::IpAddr(column)
    }
}

#[derive(Clone)]
pub struct DynamicColumnHandle {
    pub(crate) file_slice: FileSlice,
    pub(crate) column_type: ColumnType,
}

impl DynamicColumnHandle {
    pub fn open(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes()?;
        self.open_internal(column_bytes)
    }

    pub async fn open_async(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes_async().await?;
        self.open_internal(column_bytes)
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
