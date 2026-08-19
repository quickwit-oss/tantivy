use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::ast::{Function, Literal, UntypedExpr};
use crate::functions::InvalidFunctionCall;
use crate::types::VarType;

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub struct InferredTypeSet {
    pub string: bool,
    pub i64: bool,
    pub u64: bool,
    pub f64: bool,
    pub boolean: bool,
}

impl InferredTypeSet {
    pub const NONE: InferredTypeSet = InferredTypeSet {
        string: false,
        i64: false,
        u64: false,
        f64: false,
        boolean: false,
    };

    pub const ALL: InferredTypeSet = InferredTypeSet {
        string: true,
        i64: true,
        u64: true,
        f64: true,
        boolean: true,
    };

    pub const NUMERICAL: InferredTypeSet = InferredTypeSet {
        i64: true,
        u64: true,
        f64: true,
        boolean: false,
        string: false,
    };

    pub const I64: InferredTypeSet = InferredTypeSet {
        i64: true,
        ..Self::NONE
    };

    pub const U64: InferredTypeSet = InferredTypeSet {
        u64: true,
        ..Self::NONE
    };

    pub const F64: InferredTypeSet = InferredTypeSet {
        f64: true,
        ..Self::NONE
    };

    pub const STRING: InferredTypeSet = InferredTypeSet {
        string: true,
        ..Self::NONE
    };

    pub const BOOLEAN: InferredTypeSet = InferredTypeSet {
        boolean: true,
        ..Self::NONE
    };

    pub(crate) fn is_none(self) -> bool {
        self == Self::NONE
    }

    pub fn singleton(var_type: VarType) -> InferredTypeSet {
        match var_type {
            VarType::Bool => Self::BOOLEAN,
            VarType::F64 => Self::F64,
            VarType::U64 => Self::U64,
            VarType::I64 => Self::I64,
            VarType::Str => Self::STRING,
            VarType::None => Self::NONE,
        }
    }

    pub(crate) fn intersect(self, target_inferred_type: InferredTypeSet) -> InferredTypeSet {
        InferredTypeSet {
            string: self.string && target_inferred_type.string,
            i64: self.i64 && target_inferred_type.i64,
            u64: self.u64 && target_inferred_type.u64,
            f64: self.f64 && target_inferred_type.f64,
            boolean: self.boolean && target_inferred_type.boolean,
        }
    }

    pub fn contains(&self, var_type: VarType) -> bool {
        match var_type {
            VarType::Bool => self.boolean,
            VarType::F64 => self.f64,
            VarType::U64 => self.u64,
            VarType::I64 => self.i64,
            VarType::Str => self.string,
            VarType::None => self.is_none(),
        }
    }
}

impl From<VarType> for InferredTypeSet {
    fn from(var_type: VarType) -> Self {
        match var_type {
            VarType::Bool => Self::BOOLEAN,
            VarType::F64 => Self::F64,
            VarType::U64 => Self::U64,
            VarType::I64 => Self::I64,
            VarType::Str => Self::STRING,
            VarType::None => Self::NONE,
        }
    }
}

impl std::fmt::Display for InferredTypeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut types = Vec::new();
        if self.string {
            types.push("string");
        }
        if self.i64 {
            types.push("i64");
        }
        if self.u64 {
            types.push("u64");
        }
        if self.f64 {
            types.push("f64");
        }
        if self.boolean {
            types.push("boolean");
        }
        write!(f, "{{{}}}", types.join(", "))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error(transparent)]
    InvalidFunctionCall(#[from] InvalidFunctionCall),
    #[error("function `{function:?}` returns `{got}`, expected `{expected}`")]
    WrongFunctionReturnType {
        function: Function,
        expected: InferredTypeSet,
        got: InferredTypeSet,
    },
    #[error("function `{function:?}` expects `{expected}` args, was passed `{got}`")]
    InvalidNumberOfArguments {
        function: Function,
        expected: usize,
        got: usize,
    },
    #[error("expected `{expected}` , got `{literal:?}`")]
    InvalidLiteralType {
        literal: Literal,
        expected: InferredTypeSet,
    },
}

/// Infer the accepted types for the different variables present in the formula.
pub fn infer_types(expr: &UntypedExpr) -> Result<HashMap<&str, InferredTypeSet>, TypeError> {
    infer_types_with_target(expr, InferredTypeSet::ALL)
}

/// Infer the accepted variable types while constraining the expression's result type.
pub fn infer_types_with_target(
    expr: &UntypedExpr,
    target_type: InferredTypeSet,
) -> Result<HashMap<&str, InferredTypeSet>, TypeError> {
    let mut inferred_type_res = HashMap::default();
    infer_types_aux(expr, target_type, &mut inferred_type_res)?;
    Ok(inferred_type_res)
}

pub(crate) fn infer_types_aux<'a>(
    expr: &'a UntypedExpr,
    target_inferred_type: InferredTypeSet,
    inferred_types_res: &mut HashMap<&'a str, InferredTypeSet>,
) -> Result<InferredTypeSet, TypeError> {
    match expr {
        UntypedExpr::Literal(literal) => {
            let literal_type: InferredTypeSet = target_inferred_type.intersect(literal.types());
            if literal_type.is_none() {
                return Err(TypeError::InvalidLiteralType {
                    literal: literal.clone(),
                    expected: target_inferred_type,
                });
            }
            Ok(literal_type)
        }
        UntypedExpr::Variable(variable_name) => {
            match inferred_types_res.entry(variable_name.as_ref()) {
                Entry::Occupied(mut occupied_entry) => {
                    let inferred_types = occupied_entry.get().intersect(target_inferred_type);
                    occupied_entry.insert(inferred_types);
                    Ok(inferred_types)
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert_entry(target_inferred_type);
                    Ok(target_inferred_type)
                }
            }
        }
        UntypedExpr::Call { function, args } => {
            function.infer_types(args, target_inferred_type, inferred_types_res)
        }
    }
}

pub(crate) fn infer_type_with_variable_types(
    expr: &UntypedExpr,
    target_inferred_type: InferredTypeSet,
    variable_types: &HashMap<&str, VarType>,
) -> Result<InferredTypeSet, TypeError> {
    let mut inferred_types = HashMap::new();
    seed_variable_types(expr, variable_types, &mut inferred_types);
    infer_types_aux(expr, target_inferred_type, &mut inferred_types)
}

fn seed_variable_types<'a>(
    expr: &'a UntypedExpr,
    variable_types: &HashMap<&str, VarType>,
    inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
) {
    match expr {
        UntypedExpr::Literal(_) => {}
        UntypedExpr::Variable(variable_name) => {
            let inferred_type = variable_types
                .get(variable_name.as_ref())
                .copied()
                .map(InferredTypeSet::from)
                .unwrap_or(InferredTypeSet::NONE);
            inferred_types.insert(variable_name.as_ref(), inferred_type);
        }
        UntypedExpr::Call { args, .. } => {
            for arg in args {
                seed_variable_types(arg, variable_types, inferred_types);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_types_bare_variable_accepts_all() {
        // A lone variable should accept all types.
        let expr = UntypedExpr::variable("a");
        let inferred_types = infer_types(&expr).unwrap();
        let a_types = inferred_types.get("a").unwrap();
        assert_eq!(a_types, &InferredTypeSet::ALL);
    }

    #[test]
    fn test_infer_type_uses_concrete_variable_types() {
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("my_col"),
            UntypedExpr::literal(1i64),
        ]);
        let variable_types = HashMap::from([("my_col", VarType::U64)]);

        let inferred_type =
            infer_type_with_variable_types(&expr, InferredTypeSet::NUMERICAL, &variable_types)
                .unwrap();

        assert_eq!(inferred_type, InferredTypeSet::U64);
    }

    #[test]
    fn test_infer_type_falls_back_to_f64_for_disjoint_numeric_types() {
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("unsigned"),
            UntypedExpr::variable("signed"),
        ]);
        let variable_types = HashMap::from([("unsigned", VarType::U64), ("signed", VarType::I64)]);

        let inferred_type =
            infer_type_with_variable_types(&expr, InferredTypeSet::NUMERICAL, &variable_types)
                .unwrap();

        assert_eq!(inferred_type, InferredTypeSet::F64);
    }

    #[test]
    fn test_inferred_type_set_display_lists_concrete_numeric_types() {
        assert_eq!(
            InferredTypeSet::ALL.to_string(),
            "{string, i64, u64, f64, boolean}"
        );
        assert_eq!(InferredTypeSet::NUMERICAL.to_string(), "{i64, u64, f64}");
    }
}
