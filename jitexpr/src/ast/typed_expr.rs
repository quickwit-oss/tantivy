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

#[derive(Clone, PartialEq)]
pub struct TypedExpr {
    pub return_type: VarType,
    pub ast: TypedExprAst,
}

impl TypedExpr {
    pub fn none() -> TypedExpr {
        TypedExpr {
            return_type: VarType::None,
            ast: TypedExprAst::Literal(Literal::None),
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum TypedExprAst {
    Literal(Literal),
    Variable(TypedVariable),
    Coerce {
        target_type: VarType,
        expr: Box<TypedExpr>,
    },
    Call {
        function: Function,
        args: Vec<TypedExpr>,
    },
}

impl TypedExprAst {
    pub fn literal(val: impl Into<Literal>) -> TypedExprAst {
        TypedExprAst::Literal(val.into())
    }

    pub fn variable(variable_name: impl ToString, r#type: VarType) -> TypedExprAst {
        TypedExprAst::Variable(TypedVariable {
            variable_name: Arc::from(variable_name.to_string()),
            r#type,
        })
    }
}

impl From<Literal> for TypedExprAst {
    fn from(literal: Literal) -> Self {
        TypedExprAst::Literal(literal)
    }
}
