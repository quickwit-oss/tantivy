use std::sync::Arc;

#[cfg(test)]
use crate::ast::Literal;
use crate::functions::FnCallEnum;
use crate::types::{StringRef, VarType};

#[derive(Clone, PartialEq)]
pub struct TypedVariable {
    /// The source-level variable name.
    pub variable_name: Arc<str>,
    /// The concrete type expected in this input slot.
    pub r#type: VarType,
    /// The position in the compiled input array.
    pub variable_id: usize,
}

#[derive(Clone, PartialEq)]
pub(crate) struct TypedExpr {
    pub(crate) return_type: VarType,
    pub(crate) ast: TypedExprAst,
}

impl TypedExpr {
    pub(crate) fn coerce(self, target_type: VarType) -> TypedExpr {
        if target_type == self.return_type {
            self
        } else {
            let TypedExpr { return_type, ast } = self;
            let ast = match (ast, target_type) {
                (TypedExprAst::Literal(TypedLiteral::U64(value)), VarType::I64)
                    if value <= i64::MAX as u64 =>
                {
                    TypedExprAst::Literal(TypedLiteral::I64(value as i64))
                }
                (TypedExprAst::Literal(TypedLiteral::U64(value)), VarType::F64) => {
                    TypedExprAst::Literal(TypedLiteral::F64(value as f64))
                }
                (TypedExprAst::Literal(TypedLiteral::I64(value)), VarType::U64) if value >= 0 => {
                    TypedExprAst::Literal(TypedLiteral::U64(value as u64))
                }
                (TypedExprAst::Literal(TypedLiteral::I64(value)), VarType::F64) => {
                    TypedExprAst::Literal(TypedLiteral::F64(value as f64))
                }
                (TypedExprAst::Literal(TypedLiteral::F64(value)), VarType::U64)
                    if value.is_finite()
                        && value.fract() == 0.0
                        && value >= 0.0
                        && value < u64::MAX as f64 =>
                {
                    TypedExprAst::Literal(TypedLiteral::U64(value as u64))
                }
                (TypedExprAst::Literal(TypedLiteral::F64(value)), VarType::I64)
                    if value.is_finite()
                        && value.fract() == 0.0
                        && value >= i64::MIN as f64
                        && value < -(i64::MIN as f64) =>
                {
                    TypedExprAst::Literal(TypedLiteral::I64(value as i64))
                }
                (ast, target_type) => TypedExprAst::Coerce {
                    target_type,
                    expr: Box::new(TypedExpr { return_type, ast }),
                },
            };
            TypedExpr {
                return_type: target_type,
                ast,
            }
        }
    }

    pub(crate) fn none() -> TypedExpr {
        TypedExpr {
            return_type: VarType::None,
            ast: TypedExprAst::Literal(TypedLiteral::None),
        }
    }

    #[cfg(test)]
    pub(crate) fn literal(val: impl Into<Literal>) -> TypedExpr {
        let literal: Literal = val.into();
        let r#type = literal.r#type();
        let literal = match literal {
            Literal::None => TypedLiteral::None,
            Literal::Bool(value) => TypedLiteral::Bool(value),
            Literal::U64(value) => TypedLiteral::U64(value),
            Literal::I64(value) => TypedLiteral::I64(value),
            Literal::F64(value) => TypedLiteral::F64(value),
            Literal::String(_) => panic!("typed string literals require registered backing data"),
        };
        TypedExprAst::Literal(literal).with_type(r#type)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypedLiteral {
    None,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    String(StringRef<'static>),
}

impl TypedLiteral {
    pub(crate) fn r#type(&self) -> VarType {
        match self {
            TypedLiteral::None => VarType::None,
            TypedLiteral::Bool(_) => VarType::Bool,
            TypedLiteral::U64(_) => VarType::U64,
            TypedLiteral::I64(_) => VarType::I64,
            TypedLiteral::F64(_) => VarType::F64,
            TypedLiteral::String(_) => VarType::Str,
        }
    }
}

#[derive(Clone, PartialEq)]
pub(crate) enum TypedExprAst {
    Literal(TypedLiteral),
    Variable(TypedVariable),
    Coerce {
        target_type: VarType,
        expr: Box<TypedExpr>,
    },
    FnCall(FnCallEnum),
}

impl TypedExprAst {
    #[cfg(test)]
    pub(crate) fn with_type(self, return_type: VarType) -> TypedExpr {
        TypedExpr {
            return_type,
            ast: self,
        }
    }

    pub(crate) fn variable(variable_name: impl ToString, r#type: VarType) -> TypedExprAst {
        TypedExprAst::Variable(TypedVariable {
            variable_name: Arc::from(variable_name.to_string()),
            r#type,
            variable_id: 0,
        })
    }

    pub(crate) fn from_call(fn_call: impl Into<FnCallEnum>) -> TypedExprAst {
        TypedExprAst::FnCall(fn_call.into())
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
            TypedExprAst::FnCall(fn_call) => {
                write!(f, "{fn_call:?}")
            }
        }
    }
}

impl std::fmt::Debug for TypedVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{{}:{:?}}}", self.variable_name, self.r#type)
    }
}
