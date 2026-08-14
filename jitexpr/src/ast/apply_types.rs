use std::collections::HashMap;
use std::sync::Arc;

use crate::ast::typed_expr::TypedVariable;
use crate::ast::{Function, Literal, TypedExpr, TypedExprAst, UntypedExpr};
use crate::types::VarType;

/// If a variable is missing from variable_types, it will be treated as if its value is None.
pub fn apply_types(
    untyped_expr: &UntypedExpr,
    variable_types: &HashMap<&str, VarType>,
) -> (TypedExpr, Vec<TypedVariable>) {
    let mut typed_expr = apply_types_aux(untyped_expr, variable_types);
    let var_args: Vec<TypedVariable> = assign_variable_ids(&mut typed_expr);
    (typed_expr, var_args)
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
    let typed_args: Vec<TypedExpr> = typed_args
        .into_iter()
        .map(|typed_arg| typed_arg.coerce(return_type))
        .collect();
    TypedExpr {
        return_type,
        ast: Function::Add.call_typed_expr(typed_args),
    }
}

/// Walks the AST and assigns each distinct variable an auto-incremented id
/// (its offset in the input array). Repeated occurrences of the same variable
/// share the same id.
///
/// Returns the list of input variables in id order.
fn assign_variable_ids(expr: &mut TypedExpr) -> Vec<TypedVariable> {
    let mut name_to_vars: HashMap<Arc<str>, TypedVariable> = HashMap::new();
    assign_variable_ids_aux(&mut expr.ast, &mut name_to_vars);
    let mut input_vars: Vec<TypedVariable> = name_to_vars.into_values().collect();
    input_vars.sort_by_key(|var| var.variable_id);
    input_vars
}

fn assign_variable_ids_aux(
    ast: &mut TypedExprAst,
    name_to_vars: &mut HashMap<Arc<str>, TypedVariable>,
) {
    match ast {
        TypedExprAst::Literal(_) => {}
        TypedExprAst::Variable(var) => {
            if let Some(typed_var) = name_to_vars.get(&var.variable_name) {
                assert_eq!(
                    typed_var.r#type, var.r#type,
                    "variable `{}` appears with two different types (`{:?}` and `{:?}`); a typed \
                     expr AST must be built with a single explicit type per variable",
                    var.variable_name, typed_var.r#type, var.r#type,
                );
                var.variable_id = typed_var.variable_id;
            } else {
                var.variable_id = name_to_vars.len();
                name_to_vars.insert(var.variable_name.clone(), var.clone());
            };
        }
        TypedExprAst::Coerce { expr, .. } => {
            assign_variable_ids_aux(&mut expr.ast, name_to_vars);
        }
        TypedExprAst::Call { args, .. } => {
            for arg in args {
                assign_variable_ids_aux(&mut arg.ast, name_to_vars);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_types_sum_simple() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("present"),
            UntypedExpr::literal(1u64),
        ]);
        let variable_types = HashMap::from([("present", VarType::U64)]);

        let (typed_expr, _) = apply_types(&untyped_expr, &variable_types);

        assert_eq!(
            typed_expr,
            Function::Add
                .call_typed_expr(vec![
                    TypedExprAst::variable("present", VarType::U64).with_type(VarType::U64),
                    TypedExpr::literal(1u64),
                ])
                .with_type(VarType::U64)
        );
    }

    #[test]
    fn test_apply_types_sum_coercion() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("present"),
            UntypedExpr::literal(1.2f64),
        ]);
        let variable_types = HashMap::from([("present", VarType::U64)]);

        let (typed_expr, _) = apply_types(&untyped_expr, &variable_types);

        assert_eq!(
            typed_expr,
            Function::Add
                .call_typed_expr(vec![
                    TypedExprAst::variable("present", VarType::U64)
                        .with_type(VarType::U64)
                        .coerce(VarType::F64),
                    TypedExpr::literal(1.2f64),
                ])
                .with_type(VarType::F64)
        );
    }

    #[test]
    fn test_apply_types_sum_variable_missing() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("present"),
            Function::Add.call_untyped_expr(vec![
                UntypedExpr::literal(1u64),
                UntypedExpr::variable("missing"),
            ]),
        ]);
        let variable_types = HashMap::from([("present", VarType::U64)]);

        let (typed_expr, _) = apply_types(&untyped_expr, &variable_types);

        assert_eq!(typed_expr, TypedExpr::none());
    }

    #[test]
    fn test_apply_types_to_literal() {
        let untyped_expr = UntypedExpr::literal("hello");
        assert_eq!(
            apply_types(&untyped_expr, &HashMap::new()).0,
            TypedExprAst::literal("hello").with_type(VarType::Str)
        );
    }

    #[test]
    fn test_assign_variable_ids_two_variables_different_types() {
        // add(x, y) with x: U64 and y: F64. Add coerces U64 to F64, so we
        // get: Add(Coerce(x as F64), y). ids are assigned in DFS order.
        let untyped_expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("x"), UntypedExpr::variable("y")]);
        let variable_types = HashMap::from([("x", VarType::U64), ("y", VarType::F64)]);

        let (_typed_expr, var_args) = apply_types(&untyped_expr, &variable_types);

        assert_eq!(var_args.len(), 2);

        assert_eq!(var_args[0].variable_name.as_ref(), "x");
        assert_eq!(var_args[0].r#type, VarType::U64);
        assert_eq!(var_args[0].variable_id, 0);
        assert_eq!(var_args[1].variable_name.as_ref(), "y");
        assert_eq!(var_args[1].r#type, VarType::F64);
        assert_eq!(var_args[1].variable_id, 1);
    }

    #[test]
    #[should_panic(expected = "appears with two different types")]
    fn test_assign_variable_ids_panics_on_inconsistent_types() {
        // Manually build a TypedExpr where the variable `x` appears twice with
        // two different types (U64 and F64). This should never happen when the
        // tree is built via apply_types, so we panic to surface the bug.
        let mut typed_expr = Function::Add
            .call_typed_expr(vec![
                TypedExprAst::variable("x", VarType::U64).with_type(VarType::U64),
                TypedExprAst::variable("x", VarType::F64).with_type(VarType::F64),
            ])
            .with_type(VarType::F64);

        assign_variable_ids(&mut typed_expr);
    }

    #[test]
    fn test_assign_variable_ids_dedups_repeated_variable() {
        // add(x, add(y, x)) — `x` appears twice and must be assigned the same id
        // (single slot in the input array). Expected DFS traversal:
        //   x (new, id=0), y (new, id=1), x (already seen, id=0).
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            Function::Add
                .call_untyped_expr(vec![UntypedExpr::variable("y"), UntypedExpr::variable("x")]),
        ]);
        let variable_types: HashMap<&str, VarType> =
            HashMap::from([("x", VarType::U64), ("y", VarType::U64)]);

        let (_typed_expr, var_args) = apply_types(&untyped_expr, &variable_types);

        assert_eq!(var_args.len(), 2);
        assert_eq!(var_args[0].variable_name.as_ref(), "x");
        assert_eq!(var_args[0].r#type, VarType::U64);
        assert_eq!(var_args[0].variable_id, 0);
        assert_eq!(var_args[1].variable_name.as_ref(), "y");
        assert_eq!(var_args[1].r#type, VarType::U64);
        assert_eq!(var_args[1].variable_id, 1);
    }
}
