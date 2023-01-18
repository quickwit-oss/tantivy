use crate::utils::{place_bits, select_bits};
use crate::value::NumericalType;
use crate::InvalidData;

/// The column type represents the column type and can fit on 6-bits.
///
/// - bits[0..3]: Column category type.
/// - bits[3..6]: Numerical type if necessary.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum ColumnType {
    Str,
    Numerical(NumericalType),
    Bool,
    DateTime,
}

impl ColumnType {
    /// Encoded over 6 bits.
    pub(crate) fn to_code(self) -> u8 {
        let column_type_category;
        let numerical_type_code: u8;
        match self {
            ColumnType::Str => {
                column_type_category = ColumnTypeCategory::Str;
                numerical_type_code = 0u8;
            }
            ColumnType::Numerical(numerical_type) => {
                column_type_category = ColumnTypeCategory::Numerical;
                numerical_type_code = numerical_type.to_code();
            }
            ColumnType::Bool => {
                column_type_category = ColumnTypeCategory::Bool;
                numerical_type_code = 0u8;
            }
            ColumnType::DateTime => {
                column_type_category = ColumnTypeCategory::DateTime;
                numerical_type_code = 0u8;
            }
        }
        place_bits::<0, 3>(column_type_category.to_code()) | place_bits::<3, 6>(numerical_type_code)
    }

    pub(crate) fn try_from_code(code: u8) -> Result<ColumnType, InvalidData> {
        if select_bits::<6, 8>(code) != 0u8 {
            return Err(InvalidData);
        }
        let column_type_category_code = select_bits::<0, 3>(code);
        let numerical_type_code = select_bits::<3, 6>(code);
        let column_type_category = ColumnTypeCategory::try_from_code(column_type_category_code)?;
        match column_type_category {
            ColumnTypeCategory::Bool => {
                if numerical_type_code != 0u8 {
                    return Err(InvalidData);
                }
                Ok(ColumnType::Bool)
            }
            ColumnTypeCategory::Str => {
                if numerical_type_code != 0u8 {
                    return Err(InvalidData);
                }
                Ok(ColumnType::Str)
            }
            ColumnTypeCategory::Numerical => {
                let numerical_type = NumericalType::try_from_code(numerical_type_code)?;
                Ok(ColumnType::Numerical(numerical_type))
            }
            ColumnTypeCategory::DateTime => {
                if numerical_type_code != 0u8 {
                    return Err(InvalidData);
                }
                Ok(ColumnType::DateTime)
            }
        }
    }
}

pub trait HasAssociatedColumnType: 'static + Send + Sync + Copy + PartialOrd {
    fn column_type() -> ColumnType;
}

impl HasAssociatedColumnType for u64 {
    fn column_type() -> ColumnType {
        ColumnType::Numerical(NumericalType::U64)
    }
}

impl HasAssociatedColumnType for i64 {
    fn column_type() -> ColumnType {
        ColumnType::Numerical(NumericalType::I64)
    }
}

impl HasAssociatedColumnType for f64 {
    fn column_type() -> ColumnType {
        ColumnType::Numerical(NumericalType::F64)
    }
}

impl HasAssociatedColumnType for bool {
    fn column_type() -> ColumnType {
        ColumnType::Bool
    }
}

impl HasAssociatedColumnType for crate::DateTime {
    fn column_type() -> ColumnType {
        ColumnType::DateTime
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
    Bool = 0u8,
    Str = 1u8,
    Numerical = 2u8,
    DateTime = 3u8,
}

impl ColumnTypeCategory {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Result<Self, InvalidData> {
        match code {
            0u8 => Ok(Self::Bool),
            1u8 => Ok(Self::Str),
            2u8 => Ok(Self::Numerical),
            3u8 => Ok(Self::DateTime),
            _ => Err(InvalidData),
        }
    }
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
        assert_eq!(column_type_set.len(), 3 + 3);
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
