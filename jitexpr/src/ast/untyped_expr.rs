use std::sync::Arc;

use crate::ast::{Function, Literal};

/// An expression independent from its protobuf representation.
#[derive(Clone, Debug, PartialEq)]
pub enum UntypedExpr {
    Literal(Literal),
    Variable(Arc<str>),
    Call {
        function: Function,
        args: Vec<UntypedExpr>,
    },
}

impl UntypedExpr {
    pub fn literal(val: impl Into<Literal>) -> UntypedExpr {
        UntypedExpr::Literal(val.into())
    }

    pub fn variable(variable_name: impl ToString) -> UntypedExpr {
        UntypedExpr::Variable(Arc::from(variable_name.to_string()))
    }
}

impl From<Literal> for UntypedExpr {
    fn from(literal: Literal) -> Self {
        UntypedExpr::Literal(literal)
    }
}
