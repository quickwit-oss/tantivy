use crate::utils::{place_bits, select_bits};
use crate::value::NumericalType;
use crate::InvalidData;

/// Enum describing the number of values that can exist per document
/// (or per row if you will).
///
/// The cardinality must fit on 2 bits.
#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    /// All documents contain exactly one value.
    /// Required is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Required = 0,
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
            0 => Ok(Cardinality::Required),
            1 => Ok(Cardinality::Optional),
            2 => Ok(Cardinality::Multivalued),
            _ => Err(InvalidData),
        }
    }
}

/// The column type represents the column type and can fit on 6-bits.
///
/// - bits[0..3]: Column category type.
/// - bits[3..6]: Numerical type if necessary.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum ColumnType {
    Bytes,
    Numerical(NumericalType),
    Bool,
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
            _ => Err(InvalidData),
        }
    }
}

/// Represents the type and cardinality of a column.
/// This is encoded over one-byte and added to a column key in the
/// columnar sstable.
///
/// - [0..6] bits: encodes the column type
/// - [6..8] bits: encodes the cardinality
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone)]
pub struct ColumnTypeAndCardinality {
    pub typ: ColumnType,
    pub cardinality: Cardinality,
}

impl ColumnTypeAndCardinality {
    pub fn to_code(self) -> u8 {
        place_bits::<0, 6>(self.typ.to_code()) | place_bits::<6, 8>(self.cardinality.to_code())
    }

    pub fn try_from_code(code: u8) -> Result<ColumnTypeAndCardinality, InvalidData> {
        let typ_code = select_bits::<0, 6>(code);
        let cardinality_code = select_bits::<6, 8>(code);
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        let typ = ColumnType::try_from_code(typ_code)?;
        assert_eq!(typ.to_code(), typ_code);
        Ok(ColumnTypeAndCardinality { cardinality, typ })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::ColumnTypeAndCardinality;
    use crate::column_type_header::{Cardinality, ColumnType};

    #[test]
    fn test_column_type_header_to_code() {
        let mut column_type_header_set: HashSet<ColumnTypeAndCardinality> = HashSet::new();
        for code in u8::MIN..=u8::MAX {
            if let Ok(column_type_header) = ColumnTypeAndCardinality::try_from_code(code) {
                assert_eq!(column_type_header.to_code(), code);
                assert!(column_type_header_set.insert(column_type_header));
            }
        }
        assert_eq!(
            column_type_header_set.len(),
            3 /* cardinality */ *
            (1 + 1 + 3) // column_types (str, bool, numerical x 3)
        );
    }

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
