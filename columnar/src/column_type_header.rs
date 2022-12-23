use crate::utils::{place_bits, select_bits};
use crate::value::NumericalType;

/// Enum describing the number of values that can exist per document
/// (or per row if you will).
#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    /// All documents contain exactly one value.
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

    pub(crate) fn try_from_code(code: u8) -> Option<Cardinality> {
        match code {
            0 => Some(Cardinality::Required),
            1 => Some(Cardinality::Optional),
            2 => Some(Cardinality::Multivalued),
            _ => None,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum ColumnType {
    Bytes,
    Numerical(NumericalType),
    Bool,
}

impl ColumnType {
    /// Encoded over 6 bits.
    pub(crate) fn to_code(self) -> u8 {
        let high_type;
        let low_code: u8;
        match self {
            ColumnType::Bytes => {
                high_type = GeneralType::Str;
                low_code = 0u8;
            }
            ColumnType::Numerical(numerical_type) => {
                high_type = GeneralType::Numerical;
                low_code = numerical_type.to_code();
            }
            ColumnType::Bool => {
                high_type = GeneralType::Bool;
                low_code = 0u8;
            }
        }
        place_bits::<3, 6>(high_type.to_code()) | place_bits::<0, 3>(low_code)
    }

    pub(crate) fn try_from_code(code: u8) -> Option<ColumnType> {
        if select_bits::<6, 8>(code) != 0u8 {
            return None;
        }
        let high_code = select_bits::<3, 6>(code);
        let low_code = select_bits::<0, 3>(code);
        let high_type = GeneralType::try_from_code(high_code)?;
        match high_type {
            GeneralType::Bool => {
                if low_code != 0u8 {
                    return None;
                }
                Some(ColumnType::Bool)
            }
            GeneralType::Str => {
                if low_code != 0u8 {
                    return None;
                }
                Some(ColumnType::Bytes)
            }
            GeneralType::Numerical => {
                let numerical_type = NumericalType::try_from_code(low_code)?;
                Some(ColumnType::Numerical(numerical_type))
            }
        }
    }
}

/// This corresponds to the JsonType.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
#[repr(u8)]
pub(crate) enum GeneralType {
    Bool = 0u8,
    Str = 1u8,
    Numerical = 2u8,
}

impl GeneralType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Option<Self> {
        match code {
            0u8 => Some(Self::Bool),
            1u8 => Some(Self::Str),
            2u8 => Some(Self::Numerical),
            _ => None,
        }
    }
}

/// Represents the type and cardinality of a column.
/// This is encoded over one-byte and added to a column key in the
/// columnar sstable.
///
/// Cardinality is encoded as the first two highest two bits.
/// The low 6 bits encode the column type.
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone)]
pub struct ColumnTypeAndCardinality {
    pub cardinality: Cardinality,
    pub typ: ColumnType,
}

impl ColumnTypeAndCardinality {
    pub fn to_code(self) -> u8 {
        place_bits::<6, 8>(self.cardinality.to_code()) | place_bits::<0, 6>(self.typ.to_code())
    }

    pub fn try_from_code(code: u8) -> Option<ColumnTypeAndCardinality> {
        let typ_code = select_bits::<0, 6>(code);
        let cardinality_code = select_bits::<6, 8>(code);
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        let typ = ColumnType::try_from_code(typ_code)?;
        assert_eq!(typ.to_code(), typ_code);
        Some(ColumnTypeAndCardinality { cardinality, typ })
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
            if let Some(column_type_header) = ColumnTypeAndCardinality::try_from_code(code) {
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
            if let Some(column_type) = ColumnType::try_from_code(code) {
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
            let cardinality_opt = Cardinality::try_from_code(code);
            if let Some(cardinality) = cardinality_opt {
                assert_eq!(cardinality.to_code(), code);
                num_cardinality += 1;
            }
        }
        assert_eq!(num_cardinality, 3);
    }
}
