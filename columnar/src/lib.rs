#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(test)]
#[macro_use]
extern crate more_asserts;

#[cfg(all(test, feature = "unstable"))]
extern crate test;

use std::io;

mod column;
mod column_index;
mod column_values;
mod columnar;
mod dictionary;
mod dynamic_column;
pub(crate) mod utils;
mod value;

pub use column::{BytesColumn, Column, StrColumn};
pub use column_index::ColumnIndex;
pub use column_values::{ColumnValues, MonotonicallyMappableToU128, MonotonicallyMappableToU64};
pub use columnar::{
    merge_columnar, ColumnType, ColumnarReader, ColumnarWriter, HasAssociatedColumnType,
    MergeDocOrder,
};
pub use value::{NumericalType, NumericalValue};

pub use self::dynamic_column::{DynamicColumn, DynamicColumnHandle};

pub type RowId = u32;

#[derive(Clone, Copy, PartialOrd, PartialEq, Default, Debug)]
pub struct DateTime {
    pub timestamp_micros: i64,
}

#[derive(Copy, Clone, Debug)]
pub struct InvalidData;

impl From<InvalidData> for io::Error {
    fn from(_: InvalidData) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid data")
    }
}

/// Enum describing the number of values that can exist per document
/// (or per row if you will).
///
/// The cardinality must fit on 2 bits.
#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    /// All documents contain exactly one value.
    /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Full = 0,
    /// All documents contain at most one value.
    Optional = 1,
    /// All documents may contain any number of values.
    Multivalued = 2,
}

impl Cardinality {
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_code(code: u8) -> Result<Cardinality, InvalidData> {
        match code {
            0 => Ok(Cardinality::Full),
            1 => Ok(Cardinality::Optional),
            2 => Ok(Cardinality::Multivalued),
            _ => Err(InvalidData),
        }
    }
}

#[cfg(test)]
mod tests;
