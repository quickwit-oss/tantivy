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
    Bytes = 3u8,
    Str = 4u8,
    Bool = 5u8,
    IpAddr = 6u8,
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
    pub fn numerical_type(&self) -> Option<NumericalType> {
        match self {
            ColumnType::I64 => Some(NumericalType::I64),
            ColumnType::U64 => Some(NumericalType::U64),
            ColumnType::F64 => Some(NumericalType::F64),
            ColumnType::Bytes | ColumnType::Str | ColumnType::Bool | ColumnType::IpAddr => None,
        }
    }

    /// Encoded over 6 bits.
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_code(code: u8) -> Result<ColumnType, InvalidData> {
        use ColumnType::*;
        match code {
            0u8 => Ok(I64),
            1u8 => Ok(U64),
            2u8 => Ok(F64),
            3u8 => Ok(Bytes),
            4u8 => Ok(Str),
            5u8 => Ok(Bool),
            6u8 => Ok(IpAddr),
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
        assert_eq!(column_type_set.len(), 7);
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
