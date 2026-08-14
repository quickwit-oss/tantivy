mod infer_types;
mod literal;
mod typed_expr;
mod untyped_expr;

use std::collections::HashMap;

pub use infer_types::{InferredTypeSet, infer_types};
pub use literal::Literal;
pub use typed_expr::TypedExprAst;
pub use untyped_expr::UntypedExpr;

use crate::ast::typed_expr::TypedExpr;
use crate::types::VarType;

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

/// If a variable is missing from variable_types, it will be treated as if its value is None.
pub fn apply_types(
    untyped_expr: &UntypedExpr,
    variable_types: HashMap<&str, VarType>,
) -> TypedExpr {
    apply_types_aux(untyped_expr, &variable_types)
}

fn apply_types_aux(
    untyped_expr: &UntypedExpr,
    variable_types: &HashMap<&str, VarType>,
) -> TypedExpr {
    match untyped_expr {
        UntypedExpr::Literal(literal) => TypedExpr {
            return_type: literal.r#type(),
            ast: TypedExprAst::Literal(literal.clone()),
        },
        UntypedExpr::Variable(variable_name) => {
            if let Some(variable_type) = variable_types.get(variable_name.as_ref()).copied() {
                TypedExpr {
                    return_type: variable_type,
                    ast: TypedExprAst::variable(variable_name, variable_type),
                }
            } else {
                // a missing column is treated as if it was there with a constant
                // None value.
                TypedExpr {
                    return_type: VarType::None,
                    ast: TypedExprAst::Literal(Literal::None),
                }
            }
        }
        UntypedExpr::Call { function, args } => match function {
            Function::Add => apply_types_add_aux(args, variable_types),
        },
    }
}

fn apply_types_add_aux(args: &[UntypedExpr], variable_types: &HashMap<&str, VarType>) -> TypedExpr {
    let typed_args: Vec<TypedExpr> = args
        .iter()
        .map(|arg| apply_types_aux(arg, variable_types))
        .collect();

    let mut all_u64 = true;
    let mut all_i64 = true;
    for typed_arg in &typed_args {
        match typed_arg.return_type {
            VarType::U64 => all_i64 = false,
            VarType::I64 => all_u64 = false,
            VarType::F64 => {
                all_u64 = false;
                all_i64 = false;
            }
            _ => return TypedExpr::none(),
        }
    }
    let return_type = if all_u64 {
        VarType::U64
    } else if all_i64 {
        VarType::I64
    } else {
        VarType::F64
    };
    TypedExpr {
        return_type,
        ast: Function::Add.call_typed_expr(typed_args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_types_recursively() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("present"),
            Function::Add.call_untyped_expr(vec![
                UntypedExpr::literal(1u64),
                UntypedExpr::variable("missing"),
            ]),
        ]);
        let variable_types = HashMap::from([("present", VarType::U64)]);

        let typed_expr = apply_types(&untyped_expr, variable_types);

        assert_eq!(
            typed_expr,
            Function::Add.call_typed_expr(vec![
                TypedExprAst::variable("present", VarType::U64),
                Function::Add.call_typed_expr(vec![
                    TypedExprAst::literal(1u64),
                    TypedExprAst::variable("missing", VarType::None),
                ]),
            ])
        );
    }

    #[test]
    fn test_apply_types_to_literal() {
        let untyped_expr = UntypedExpr::literal("hello");

        assert_eq!(
            apply_types(&untyped_expr, HashMap::new()),
            TypedExprAst::literal("hello")
        );
    }
}
