pub mod ast;
pub mod compile;
pub mod types;

mod functions;

#[cfg(test)]
pub(crate) fn typed_expr_from_str(
    untyped_expr: &str,
    variable_types: &std::collections::HashMap<&str, types::VarType>,
) -> compile::TypedExpr {
    let untyped_expr = ast::deserialize(untyped_expr).unwrap();
    let mut context = compile::CompileFnBuilder::new(variable_types);
    context
        .apply_types(&untyped_expr, ast::InferredTypeSet::ALL)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::types::VarType;

    #[test]
    fn test_typed_expr_from_str() {
        let variable_types = HashMap::from([("value", VarType::U64)]);

        let typed_expr = typed_expr_from_str("(ADD value 1i64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::U64);
    }
}
