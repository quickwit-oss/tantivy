use crate::utils::{place_bits, select_bits};
use crate::value::NumericalType;
use crate::InvalidData;

/// The column type represents the column type and can fit on 6-bits.
///
/// - bits[0..3]: Column category type.
/// - bits[3..6]: Numerical type if necessary.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum ColumnType {
    Bytes,
    Numerical(NumericalType),
    Bool,
    IpAddr,
}

impl ColumnType {
    /// Encoded over 6 bits.
    pub(crate) fn to_code(self) -> u8 {
        let column_type_category;
        let numerical_type_code: u8;
        match self {
            ColumnType::Bytes => {
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
            ColumnType::IpAddr => {
                column_type_category = ColumnTypeCategory::IpAddr;
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
            ColumnTypeCategory::IpAddr => {
                if numerical_type_code != 0u8 {
                    return Err(InvalidData);
                }
                Ok(ColumnType::IpAddr)
            }
            ColumnTypeCategory::Str => {
                if numerical_type_code != 0u8 {
                    return Err(InvalidData);
                }
                Ok(ColumnType::Bytes)
            }
            ColumnTypeCategory::Numerical => {
                let numerical_type = NumericalType::try_from_code(numerical_type_code)?;
                Ok(ColumnType::Numerical(numerical_type))
            }
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
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
#[repr(u8)]
pub(crate) enum ColumnTypeCategory {
    Bool = 0u8,
    Str = 1u8,
    Numerical = 2u8,
    IpAddr = 3u8,
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
            3u8 => Ok(Self::IpAddr),
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
        assert_eq!(column_type_set.len(), 2 + 3);
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
