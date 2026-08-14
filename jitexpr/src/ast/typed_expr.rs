use std::sync::Arc;

use crate::ast::{Function, Literal};
use crate::types::VarType;

#[derive(Clone, PartialEq)]
pub struct TypedVariable {
    pub(super) variable_name: Arc<str>,
    pub(super) r#type: VarType,
    pub(super) variable_id: usize, //< offset in the input array.
}

#[derive(Clone, PartialEq)]
pub struct TypedExpr {
    pub return_type: VarType,
    pub ast: TypedExprAst,
}

impl TypedExpr {
    pub fn coerce(self, target_type: VarType) -> TypedExpr {
        if target_type == self.return_type {
            self
        } else {
            TypedExpr {
                return_type: target_type,
                ast: TypedExprAst::Coerce {
                    target_type,
                    expr: Box::new(self),
                },
            }
        }
    }

    pub fn none() -> TypedExpr {
        TypedExpr {
            return_type: VarType::None,
            ast: TypedExprAst::Literal(Literal::None),
        }
    }

    pub fn literal(val: impl Into<Literal>) -> TypedExpr {
        let literal: Literal = val.into();
        let r#type = literal.r#type();
        TypedExprAst::Literal(literal).with_type(r#type)
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
    pub fn with_type(self, return_type: VarType) -> TypedExpr {
        TypedExpr {
            return_type,
            ast: self,
        }
    }

    pub fn literal(val: impl Into<Literal>) -> TypedExprAst {
        TypedExprAst::Literal(val.into())
    }

    pub fn variable(variable_name: impl ToString, r#type: VarType) -> TypedExprAst {
        TypedExprAst::Variable(TypedVariable {
            variable_name: Arc::from(variable_name.to_string()),
            r#type,
            variable_id: 0,
        })
    }
}

// ---------- boilerplate ---------

impl From<Literal> for TypedExprAst {
    fn from(literal: Literal) -> Self {
        TypedExprAst::Literal(literal)
    }
}

impl std::fmt::Debug for TypedExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({:?} : {:?})", self.ast, self.return_type)
    }
}

impl std::fmt::Debug for TypedExprAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TypedExprAst::Literal(literal) => write!(f, "{:?}", literal),
            TypedExprAst::Variable(variable) => write!(f, "{:?}", variable),
            TypedExprAst::Coerce { target_type, expr } => {
                write!(f, "coerce({:?} as {:?})", expr, target_type)
            }
            TypedExprAst::Call { function, args } => {
                write!(f, "{:?}(", function)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", arg)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl std::fmt::Debug for TypedVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{{}:{:?}}}", self.variable_name, self.r#type)
    }
}
