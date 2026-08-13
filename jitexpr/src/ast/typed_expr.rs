use std::sync::Arc;

use crate::ast::{Function, Literal};
use crate::types::VarType;

#[derive(Clone, PartialEq)]
pub struct TypedVariable {
    variable_name: Arc<str>,
    r#type: VarType,
}

impl std::fmt::Debug for TypedVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}:{:?}}}", self.variable_name, self.r#type)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedExpr {
    Literal(Literal),
    Variable(TypedVariable),
    Call {
        function: Function,
        args: Vec<TypedExpr>,
    },
}

impl TypedExpr {
    pub fn literal(val: impl Into<Literal>) -> TypedExpr {
        TypedExpr::Literal(val.into())
    }

    pub fn variable(variable_name: impl ToString, r#type: VarType) -> TypedExpr {
        TypedExpr::Variable(TypedVariable {
            variable_name: Arc::from(variable_name.to_string()),
            r#type,
        })
    }
}

impl From<Literal> for TypedExpr {
    fn from(literal: Literal) -> Self {
        TypedExpr::Literal(literal)
    }
}
