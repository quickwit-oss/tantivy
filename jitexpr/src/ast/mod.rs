mod literal;
mod typed_expr;
mod untyped_expr;

use std::collections::HashMap;

pub use literal::Literal;
pub use typed_expr::TypedExpr;
pub use untyped_expr::UntypedExpr;

use crate::types::VarType;

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    Add,
}

impl Function {
    pub fn call_typed_expr(&self, args: Vec<TypedExpr>) -> TypedExpr {
        TypedExpr::Call {
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

/// If a variable is missing from variable_types, it will be treated as if its value is None.
pub fn apply_types(
    untyped_expr: &UntypedExpr,
    variable_types: HashMap<&str, VarType>,
) -> TypedExpr {
    todo!()
}
