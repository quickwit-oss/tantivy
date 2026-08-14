mod apply_types;
mod infer_types;
mod literal;
mod typed_expr;
mod untyped_expr;

pub use apply_types::apply_types;
pub use infer_types::{InferredTypeSet, infer_types};
pub use literal::Literal;
pub use typed_expr::{TypedExpr, TypedExprAst, TypedVariable};
pub use untyped_expr::UntypedExpr;

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
