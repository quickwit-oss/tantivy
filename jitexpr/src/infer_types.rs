use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::ast::{Function, Literal, UntypedExpr};

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub struct InferredTypeSet {
    string: bool,
    numerical: bool,
    boolean: bool,
}

impl InferredTypeSet {
    pub const NONE: InferredTypeSet = InferredTypeSet {
        string: false,
        numerical: false,
        boolean: false,
    };

    pub const ALL: InferredTypeSet = InferredTypeSet {
        string: true,
        numerical: true,
        boolean: true,
    };

    pub const NUMERICAL: InferredTypeSet = InferredTypeSet {
        numerical: true,
        boolean: false,
        string: false,
    };

    pub const STRING: InferredTypeSet = InferredTypeSet {
        numerical: false,
        boolean: false,
        string: true,
    };

    fn is_none(self) -> bool {
        self == Self::NONE
    }

    fn intersect(self, target_inferred_type: InferredTypeSet) -> InferredTypeSet {
        InferredTypeSet {
            string: self.string && target_inferred_type.string,
            numerical: self.numerical && target_inferred_type.numerical,
            boolean: self.boolean && target_inferred_type.boolean,
        }
    }
}

impl std::fmt::Display for InferredTypeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut types = Vec::new();
        if self.string {
            types.push("string");
        }
        if self.numerical {
            types.push("numerical");
        }
        if self.boolean {
            types.push("boolean");
        }
        write!(f, "{{{}}}", types.join(", "))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("function `{function:?}` returns a number, expected `{expected}`")]
    WrongFunctionReturnType {
        function: Function,
        expected: InferredTypeSet,
    },
    #[error("expected `{expected}` , got `{literal:?}`")]
    InvalidLiteralType {
        literal: Literal,
        expected: InferredTypeSet,
    },
}

/// Infer the accepted types for the different variables present in the formula.
pub fn infer_types<'a>(
    expr: &'a UntypedExpr,
) -> Result<HashMap<&'a str, InferredTypeSet>, TypeError> {
    let mut inferred_type_res = HashMap::default();
    infer_types_aux(expr, InferredTypeSet::ALL, &mut inferred_type_res)?;
    Ok(inferred_type_res)
}

fn infer_types_aux<'a>(
    expr: &'a UntypedExpr,
    target_inferred_type: InferredTypeSet,
    inferred_types_res: &mut HashMap<&'a str, InferredTypeSet>,
) -> Result<InferredTypeSet, TypeError> {
    match expr {
        UntypedExpr::Literal(literal) => {
            let literal_type: InferredTypeSet =
                target_inferred_type.intersect(literal_types(literal));
            if literal_type.is_none() {
                return Err(TypeError::InvalidLiteralType {
                    literal: literal.clone(),
                    expected: target_inferred_type,
                });
            }
            Ok(literal_type)
        }
        UntypedExpr::Variable(variable_name) => match inferred_types_res.entry(&*variable_name) {
            Entry::Occupied(mut occupied_entry) => {
                let inferred_types = occupied_entry.get().intersect(target_inferred_type);
                occupied_entry.insert(inferred_types);
                Ok(inferred_types)
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert_entry(target_inferred_type);
                Ok(target_inferred_type)
            }
        },
        UntypedExpr::Call { function, args } => infer_types_function_aux(
            *function,
            &args[..],
            target_inferred_type,
            inferred_types_res,
        ),
    }
}

fn infer_types_function_aux<'a>(
    function: Function,
    args: &'a [UntypedExpr],
    target_inferred_type: InferredTypeSet,
    inferred_types_res: &mut HashMap<&'a str, InferredTypeSet>,
) -> Result<InferredTypeSet, TypeError> {
    match function {
        Function::Add => {
            // This is valid for all functions taking a bunch of number and returning a number.
            if !target_inferred_type.numerical {
                return Err(TypeError::WrongFunctionReturnType {
                    function,
                    expected: target_inferred_type,
                });
            }
            for arg in args {
                infer_types_aux(arg, InferredTypeSet::NUMERICAL, inferred_types_res)?;
            }
            Ok(InferredTypeSet::NUMERICAL)
        }
    }
}

fn literal_types<'a>(literal: &'a Literal) -> InferredTypeSet {
    match literal {
        Literal::Bool(_) => InferredTypeSet {
            boolean: true,
            ..Default::default()
        },
        Literal::I64(_) | Literal::U64(_) | Literal::F64(_) => InferredTypeSet::NUMERICAL,
        Literal::String(_) => InferredTypeSet::STRING,
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::ast::{Function, Literal, UntypedExpr};

    #[test]
    fn test_infer_types_add_string_and_float_returns_error() {
        // add(1.0, "hello") should fail because a string cannot be numerical.
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1.0),
            UntypedExpr::literal("hello"),
        ]);
        let err = infer_types(&expr).unwrap_err();
        assert_matches!(
            err,
            TypeError::InvalidLiteralType {
                literal: Literal::String(_),
                expected: InferredTypeSet {
                    string: false,
                    numerical: true,
                    boolean: false,
                },
            }
        );
    }

    #[test]
    fn test_infer_types_add_literal_and_variable() {
        // add(1, a) should infer that `a` is numerical.
        let expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::literal(1i64), UntypedExpr::variable("a")]);
        let inferred_types = infer_types(&expr).unwrap();
        let a_types = inferred_types.get("a").unwrap();
        assert!(a_types.numerical);
        assert!(!a_types.string);
        assert!(!a_types.boolean);
    }

    #[test]
    fn test_infer_types_add_heterogenous_literals() {
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1.2f64),
            UntypedExpr::literal(2u64),
        ]);
        assert!(infer_types(&expr).is_ok());
    }

    #[test]
    fn test_infer_types_add_two_variables() {
        // add(a, b) should infer that both `a` and `b` are numerical.
        let expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("a"), UntypedExpr::variable("b")]);
        let inferred_types = infer_types(&expr).unwrap();

        let a_types = inferred_types.get("a").unwrap();
        assert_eq!(a_types, &InferredTypeSet::NUMERICAL);
        let b_types = inferred_types.get("b").unwrap();
        assert_eq!(b_types, &InferredTypeSet::NUMERICAL);
    }

    #[test]
    fn test_infer_types_bare_variable_accepts_all() {
        // A lone variable should accept all types.
        let expr = UntypedExpr::variable("a");
        let inferred_types = infer_types(&expr).unwrap();
        let a_types = inferred_types.get("a").unwrap();
        assert_eq!(a_types, &InferredTypeSet::ALL);
    }
}
