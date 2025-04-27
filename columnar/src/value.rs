use common::DateTime;

use crate::InvalidData;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum NumericalValue {
    I64(i64),
    U64(u64),
    F64(f64),
}

impl NumericalValue {
    pub fn numerical_type(&self) -> NumericalType {
        match self {
            Self::I64(_) => NumericalType::I64,
            Self::U64(_) => NumericalType::U64,
            Self::F64(_) => NumericalType::F64,
        }
    }

    /// Tries to normalize the numerical value in the following priorities:
    /// i64, i64, f64
    pub fn normalize(self) -> Self {
        match self {
            Self::U64(val) => {
                if val <= i64::MAX as u64 {
                    Self::I64(val as i64)
                } else {
                    Self::F64(val as f64)
                }
            }
            Self::I64(val) => Self::I64(val),
            Self::F64(val) => {
                let fract = val.fract();
                if fract == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
                    Self::I64(val as i64)
                } else if fract == 0.0 && val >= u64::MIN as f64 && val <= u64::MAX as f64 {
                    Self::U64(val as u64)
                } else {
                    Self::F64(val)
                }
            }
        }
    }
}

impl From<u64> for NumericalValue {
    fn from(val: u64) -> Self {
        Self::U64(val)
    }
}

impl From<i64> for NumericalValue {
    fn from(val: i64) -> Self {
        Self::I64(val)
    }
}

impl From<f64> for NumericalValue {
    fn from(val: f64) -> Self {
        Self::F64(val)
    }
}

#[derive(Clone, Copy, Debug, Default, Hash, Eq, PartialEq)]
#[repr(u8)]
pub enum NumericalType {
    #[default]
    I64 = 0,
    U64 = 1,
    F64 = 2,
}

impl NumericalType {
    pub fn to_code(self) -> u8 {
        self as u8
    }

    pub fn try_from_code(code: u8) -> Result<Self, InvalidData> {
        match code {
            0 => Ok(Self::I64),
            1 => Ok(Self::U64),
            2 => Ok(Self::F64),
            _ => Err(InvalidData),
        }
    }
}

/// We voluntarily avoid using `Into` here to keep this
/// implementation quirk as private as possible.
///
/// # Panics
/// This coercion trait actually panics if it is used
/// to convert a loose types to a stricter type.
///
/// The level is strictness is somewhat arbitrary.
/// - i64
/// - u64
/// - f64.
pub(crate) trait Coerce {
    fn coerce(numerical_value: NumericalValue) -> Self;
}

impl Coerce for i64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val,
            NumericalValue::U64(val) => val as Self,
            NumericalValue::F64(_) => unreachable!(),
        }
    }
}

impl Coerce for u64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val as Self,
            NumericalValue::U64(val) => val,
            NumericalValue::F64(_) => unreachable!(),
        }
    }
}

impl Coerce for f64 {
    fn coerce(value: NumericalValue) -> Self {
        match value {
            NumericalValue::I64(val) => val as Self,
            NumericalValue::U64(val) => val as Self,
            NumericalValue::F64(val) => val,
        }
    }
}

impl Coerce for DateTime {
    fn coerce(value: NumericalValue) -> Self {
        let timestamp_micros = i64::coerce(value);
        Self::from_timestamp_nanos(timestamp_micros)
    }
}

#[cfg(test)]
mod tests {
    use super::NumericalType;

    #[test]
    fn test_numerical_type_code() {
        let mut num_numerical_type = 0;
        for code in u8::MIN..=u8::MAX {
            if let Ok(numerical_type) = NumericalType::try_from_code(code) {
                assert_eq!(numerical_type.to_code(), code);
                num_numerical_type += 1;
            }
        }
        assert_eq!(num_numerical_type, 3);
    }
}
