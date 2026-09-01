//! `GT` tests whether one value is greater than another.
//!
//! It accepts exactly two operands. Ordered operands are strings or numbers; booleans are rejected.
//! Strings use lexicographic UTF-8 ordering. Numeric comparisons support `i64`, `u64`, and `f64`
//! combinations without converting large integers through a lossy `f64`. IEEE unordered
//! comparisons involving NaN return `false`.
//!
//! Null propagation is strict: if either operand is absent, the result is absent.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

use super::comparison::{self, OrderedComparison};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GtFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for GtFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::Exactly(2);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        comparison::infer_types(Function::Gt, args, target_type, inferred_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        Self::ARG_COUNT.validate(args)?;
        debug_assert!(target_type_set.contains(VarType::Bool));
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(GtFnCall {
                args: comparison::apply_types(args, context)?,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("GT", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        comparison::lower(&self.args, OrderedComparison::GreaterThan, context, builder)
    }
}

impl From<GtFnCall> for FnCallEnum {
    fn from(call: GtFnCall) -> Self {
        FnCallEnum::Gt(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<bool> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        // SAFETY: These expressions have no inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_requires_two_ordered_arguments() {
        let expression = deserialize("(GT left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        let left = inferred_types.get("left").unwrap();
        assert!(left.string && left.i64 && left.u64 && left.f64 && !left.boolean);

        for expression in ["(GT 1i64)", "(GT 1i64 2i64 3i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Gt,
                    expected: 2,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_string_and_same_type_numeric_ordering() {
        assert_eq!(eval(r#"(GT "beta" "alpha")"#), Some(true));
        assert_eq!(eval(r#"(GT "é" "z")"#), Some(true));
        assert_eq!(eval("(GT -1i64 0i64)"), Some(false));
        assert_eq!(eval("(GT 3.5f64 3f64)"), Some(true));
        assert_eq!(eval("(GT nanf64 0f64)"), Some(false));
        assert_eq!(eval("(GT nanf64 0i64)"), Some(false));
        assert_eq!(eval("(GT 0i64 nanf64)"), Some(false));
    }

    #[test]
    fn test_mixed_numeric_boundaries_are_exact() {
        assert_eq!(eval("(GT -1i64 18446744073709551615u64)"), Some(false));
        assert_eq!(eval("(GT 18446744073709551615u64 -1i64)"), Some(true));

        let expression = deserialize("(GT float integer)").unwrap();
        let variable_types = HashMap::from([("float", VarType::F64), ("integer", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[
                        VariableValue::some((1u64 << 53) as f64),
                        VariableValue::some((1u64 << 53) + 1),
                    ])
                    .as_bool()
            },
            Some(false)
        );
    }

    #[test]
    fn test_null_propagates() {
        assert_eq!(eval("(GT none 1i64)"), None);

        let expression = deserialize("(GT left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::some("a")])
                    .as_bool()
            },
            None
        );
    }
}
