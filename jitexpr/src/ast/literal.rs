use std::sync::Arc;

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
