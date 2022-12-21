use crate::value::NumericalType;

#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    #[default]
    Required = 0,
    Optional = 1,
    Multivalued = 2,
}

impl Cardinality {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Option<Cardinality> {
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
}

impl ColumnType {
    pub fn to_code(self) -> u8 {
        match self {
            ColumnType::Bytes => 0u8,
            ColumnType::Numerical(numerical_type) => 1u8 | (numerical_type.to_code() << 1),
        }
    }

    pub fn try_from_code(code: u8) -> Option<ColumnType> {
        if code == 0u8 {
            return Some(ColumnType::Bytes);
        }
        if code & 1u8 == 0u8 {
            return None;
        }
        let numerical_type = NumericalType::try_from_code(code >> 1)?;
        Some(ColumnType::Numerical(numerical_type))
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

#[inline]
const fn compute_mask(num_bits: u8) -> u8 {
    if num_bits == 8 {
        u8::MAX
    } else {
        (1u8 << num_bits) - 1
    }
}

#[inline]
fn select_bits<const START: u8, const END: u8>(code: u8) -> u8 {
    assert!(START <= END);
    assert!(END <= 8);
    let num_bits: u8 = END - START;
    let mask: u8 = compute_mask(num_bits);
    (code >> START) & mask
}

#[inline]
fn place_bits<const START: u8, const END: u8>(code: u8) -> u8 {
    assert!(START <= END);
    assert!(END <= 8);
    let num_bits: u8 = END - START;
    let mask: u8 = compute_mask(num_bits);
    assert!(code <= mask);
    code << START
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
            3 /* cardinality */ * (1 + 3) // column_types
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
        assert_eq!(column_type_set.len(), 1 + 3);
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
