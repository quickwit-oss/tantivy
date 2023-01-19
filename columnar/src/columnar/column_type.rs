use std::net::Ipv6Addr;

use crate::value::NumericalType;
use crate::InvalidData;

/// The column type represents the column type and can fit on 6-bits.
///
/// - bits[0..3]: Column category type.
/// - bits[3..6]: Numerical type if necessary.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum ColumnType {
    I64 = 0u8,
    U64 = 1u8,
    F64 = 2u8,
    Bytes = 10u8,
    Str = 14u8,
    Bool = 18u8,
    IpAddr = 22u8,
    DateTime = 26u8,
}

#[cfg(test)]
const COLUMN_TYPES: [ColumnType; 8] = [
    ColumnType::I64,
    ColumnType::U64,
    ColumnType::F64,
    ColumnType::Bytes,
    ColumnType::Str,
    ColumnType::Bool,
    ColumnType::IpAddr,
    ColumnType::DateTime,
];

impl ColumnType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_code(code: u8) -> Result<ColumnType, InvalidData> {
        use ColumnType::*;
        match code {
            0u8 => Ok(I64),
            1u8 => Ok(U64),
            2u8 => Ok(F64),
            10u8 => Ok(Bytes),
            14u8 => Ok(Str),
            18u8 => Ok(Bool),
            22u8 => Ok(IpAddr),
            26u8 => Ok(Self::DateTime),
            _ => Err(InvalidData),
        }
    }
}

impl From<NumericalType> for ColumnType {
    fn from(numerical_type: NumericalType) -> Self {
        match numerical_type {
            NumericalType::I64 => ColumnType::I64,
            NumericalType::U64 => ColumnType::U64,
            NumericalType::F64 => ColumnType::F64,
        }
    }
}

impl ColumnType {
    /// get column type category
    pub(crate) fn column_type_category(self) -> ColumnTypeCategory {
        match self {
            ColumnType::I64 | ColumnType::U64 | ColumnType::F64 => ColumnTypeCategory::Numerical,
            ColumnType::Bytes => ColumnTypeCategory::Bytes,
            ColumnType::Str => ColumnTypeCategory::Str,
            ColumnType::Bool => ColumnTypeCategory::Bool,
            ColumnType::IpAddr => ColumnTypeCategory::IpAddr,
        }
    }

    pub fn numerical_type(&self) -> Option<NumericalType> {
        match self {
            ColumnType::I64 => Some(NumericalType::I64),
            ColumnType::U64 => Some(NumericalType::U64),
            ColumnType::F64 => Some(NumericalType::F64),
            ColumnType::Bytes
            | ColumnType::Str
            | ColumnType::Bool
            | ColumnType::IpAddr
            | ColumnType::DateTime => None,
        }
    }
}

// TODO remove if possible
pub trait HasAssociatedColumnType: 'static + Send + Sync + Copy + PartialOrd {
    fn column_type() -> ColumnType;
    fn default_value() -> Self;
}

impl HasAssociatedColumnType for u64 {
    fn column_type() -> ColumnType {
        ColumnType::U64
    }

    fn default_value() -> Self {
        0u64
    }
}

impl HasAssociatedColumnType for i64 {
    fn column_type() -> ColumnType {
        ColumnType::I64
    }

    fn default_value() -> Self {
        0i64
    }
}

impl HasAssociatedColumnType for f64 {
    fn column_type() -> ColumnType {
        ColumnType::F64
    }

    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for bool {
    fn column_type() -> ColumnType {
        ColumnType::Bool
    }
    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for crate::DateTime {
    fn column_type() -> ColumnType {
        ColumnType::DateTime
    }
    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for Ipv6Addr {
    fn column_type() -> ColumnType {
        ColumnType::IpAddr
    }

    fn default_value() -> Self {
        Ipv6Addr::from([0u8; 16])
    }
}

/// Column types are grouped into different categories that
/// corresponds to the different types of `JsonValue` types.
///
/// The columnar writer will apply coercion rules to make sure that
/// at most one column exist per `ColumnTypeCategory`.
///
/// See also [README.md].
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
#[repr(u8)]
pub(crate) enum ColumnTypeCategory {
    Bool,
    Str,
    Numerical,
    DateTime,
    Bytes,
    IpAddr,
}

impl From<ColumnType> for ColumnTypeCategory {
    fn from(column_type: ColumnType) -> Self {
        match column_type {
            ColumnType::I64 => ColumnTypeCategory::Numerical,
            ColumnType::U64 => ColumnTypeCategory::Numerical,
            ColumnType::F64 => ColumnTypeCategory::Numerical,
            ColumnType::Bytes => ColumnTypeCategory::Bytes,
            ColumnType::Str => ColumnTypeCategory::Str,
            ColumnType::Bool => ColumnTypeCategory::Bool,
            ColumnType::IpAddr => ColumnTypeCategory::IpAddr,
            ColumnType::DateTime => ColumnTypeCategory::DateTime,
        }
    }
}

/// Column types are grouped into different categories that
/// corresponds to the different types of `JsonValue` types.
///
/// The columnar writer will apply coercion rules to make sure that
/// at most one column exist per `ColumnTypeCategory`.
///
/// See also [README.md].
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
#[repr(u8)]
pub enum ColumnTypeCategory {
    Bool = 0u8,
    Str = 1u8,
    Numerical = 2u8,
    IpAddr = 3u8,
    Bytes = 4u8,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::Cardinality;

    #[test]
    fn test_column_type_to_code() {
        let mut column_type_set: HashSet<ColumnType> = HashSet::new();
        for code in u8::MIN..=u8::MAX {
            if let Ok(column_type) = ColumnType::try_from_code(code) {
                assert_eq!(column_type.to_code(), code);
                assert!(column_type_set.insert(column_type));
            }
        }
        assert_eq!(column_type_set.len(), super::COLUMN_TYPES.len());
    }

    #[test]
    fn test_column_category_sort_consistent_with_column_type_sort() {
        // This is a very important property because we
        // we need to serialize colunmn in the right order.
        let mut column_types: Vec<ColumnType> = super::COLUMN_TYPES.iter().copied().collect();
        column_types.sort_by_key(|col| col.to_code());
        let column_categories: Vec<ColumnTypeCategory> = column_types
            .into_iter()
            .map(ColumnTypeCategory::from)
            .collect();
        for (prev, next) in column_categories.iter().zip(column_categories.iter()) {
            assert!(prev <= next);
        }
    }

    #[test]
    fn test_cardinality_to_code() {
        let mut num_cardinality = 0;
        for code in u8::MIN..=u8::MAX {
            if let Ok(cardinality) = Cardinality::try_from_code(code) {
                assert_eq!(cardinality.to_code(), code);
                num_cardinality += 1;
            }
        }
        assert_eq!(num_cardinality, 3);
    }
}
