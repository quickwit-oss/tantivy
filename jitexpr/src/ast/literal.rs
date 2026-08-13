use std::sync::Arc;

/// A literal supported by the first expression-language milestone.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    String(Arc<str>),
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
