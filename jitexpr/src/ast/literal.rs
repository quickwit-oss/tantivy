use std::sync::Arc;

use crate::ast::InferredTypeSet;
use crate::types::VarType;

/// A literal supported by the first expression-language milestone.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    None,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    String(Arc<str>),
}

impl Literal {
    pub fn is_none(&self) -> bool {
        matches!(self, Literal::None)
    }

    pub fn types(&self) -> InferredTypeSet {
        match self {
            Literal::None => InferredTypeSet::ALL,
            Literal::Bool(_) => InferredTypeSet::BOOLEAN,
            // A literal number represents a "real number". It can sometime be represented by a i64,
            // a u64 or a f64. The choice of this representation is rather arbitrary. It
            // can be the result of an implementation detail of serde_json for instance.
            //
            // Here we want to return the set of possible representation for the associated number.
            Literal::I64(value) => InferredTypeSet {
                i64: true,
                u64: *value >= 0, // Any non-negative i64 can be represented as u64.
                f64: true,        // We always accept f64.
                ..InferredTypeSet::NONE
            },
            Literal::U64(value) => InferredTypeSet {
                i64: *value <= i64::MAX as u64, // any u64 below i64::MAX can be represented as a
                // i64.
                u64: true,
                f64: true, // We always accept f64
                ..InferredTypeSet::NONE
            },
            Literal::F64(value) => {
                let is_integral = value.is_finite() && value.fract() == 0.0;
                InferredTypeSet {
                    i64: is_integral && *value >= i64::MIN as f64 && *value < -(i64::MIN as f64),
                    u64: is_integral && *value >= 0.0 && *value < u64::MAX as f64,
                    f64: true,
                    ..InferredTypeSet::NONE
                }
            }
            Literal::String(_) => InferredTypeSet::STRING,
        }
    }

    // TODO let's remove it
    pub fn r#type(&self) -> VarType {
        match self {
            Literal::None => VarType::None,
            Literal::Bool(_) => VarType::Bool,
            Literal::U64(_) => VarType::U64,
            Literal::I64(_) => VarType::I64,
            Literal::F64(_) => VarType::F64,
            Literal::String(_) => VarType::Str,
        }
    }
}

impl From<bool> for Literal {
    fn from(value: bool) -> Self {
        Literal::Bool(value)
    }
}

impl From<u64> for Literal {
    fn from(value: u64) -> Self {
        Literal::U64(value)
    }
}

impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Literal::I64(value)
    }
}

impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Literal::F64(value)
    }
}

impl From<String> for Literal {
    fn from(value: String) -> Self {
        Literal::String(Arc::from(value))
    }
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Literal::String(Arc::from(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_types_depend_on_representable_value() {
        let i64_f64 = InferredTypeSet {
            i64: true,
            f64: true,
            ..InferredTypeSet::NONE
        };
        let u64_f64 = InferredTypeSet {
            u64: true,
            f64: true,
            ..InferredTypeSet::NONE
        };

        assert_eq!(Literal::U64(1).types(), InferredTypeSet::NUMERICAL);
        assert_eq!(Literal::I64(1).types(), InferredTypeSet::NUMERICAL);
        assert_eq!(Literal::I64(-1).types(), i64_f64);
        assert_eq!(Literal::U64(1 << 63).types(), u64_f64);
        assert_eq!(Literal::F64(1.2).types(), InferredTypeSet::F64);
        assert_eq!(Literal::F64(1.0).types(), InferredTypeSet::NUMERICAL);
    }

    #[test]
    fn test_literal_types_accept_lossless_float_representation() {
        assert_eq!(
            Literal::I64((1 << 53) + 1).types(),
            InferredTypeSet::NUMERICAL
        );

        // even though i64::MAX - 1i64 cannot be represented as f64 in a lossless manner...
        assert_ne!(((i64::MAX - 1i64) as f64) as i64, (i64::MAX - 1));
        // ... we list f64 as a valid inferred type.
        assert_eq!(
            Literal::I64(i64::MAX - 1).types(),
            InferredTypeSet::NUMERICAL
        );
        assert_eq!(
            Literal::U64(u64::MAX).types(),
            InferredTypeSet {
                u64: true,
                f64: true,
                ..InferredTypeSet::NONE
            }
        );
        assert_eq!(
            Literal::I64(i64::MIN).types(),
            InferredTypeSet {
                i64: true,
                f64: true,
                ..InferredTypeSet::NONE
            }
        );
    }

    #[test]
    fn test_f64_literal_types_handle_integer_boundaries_and_special_values() {
        assert_eq!(
            Literal::F64(2f64.powi(63)).types(),
            InferredTypeSet {
                u64: true,
                f64: true,
                ..InferredTypeSet::NONE
            }
        );
        assert_eq!(Literal::F64(2f64.powi(64)).types(), InferredTypeSet::F64);
        assert_eq!(Literal::F64(-0.0).types(), InferredTypeSet::NUMERICAL);
        assert_eq!(Literal::F64(f64::NAN).types(), InferredTypeSet::F64);
        assert_eq!(Literal::F64(f64::INFINITY).types(), InferredTypeSet::F64);
        assert_eq!(
            Literal::F64(f64::NEG_INFINITY).types(),
            InferredTypeSet::F64
        );
    }
}
