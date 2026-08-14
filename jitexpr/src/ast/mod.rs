mod infer_types;
mod literal;
mod untyped_expr;

pub use infer_types::{InferredTypeSet, infer_types};
pub use literal::Literal;
pub use untyped_expr::UntypedExpr;

use crate::compile::{TypedExpr, TypedExprAst};

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    Add,
}

impl Function {
    pub fn call_typed_expr(&self, args: Vec<TypedExpr>) -> TypedExprAst {
        TypedExprAst::Call {
            function: *self,
            args,
        }
    }

    pub fn call_untyped_expr(&self, args: Vec<UntypedExpr>) -> UntypedExpr {
        UntypedExpr::Call {
            function: *self,
            args,
        }
    }
}
