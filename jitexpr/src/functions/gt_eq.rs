//! `GT_EQ` tests whether one value is greater than or equal to another.
//!
//! It accepts exactly two operands. Ordered operands are strings or numbers; booleans are rejected.
//! Strings use lexicographic UTF-8 ordering. Numeric comparisons support `i64`, `u64`, and `f64`
//! combinations without converting large integers through a lossy `f64`. IEEE unordered
//! comparisons involving NaN return `false`, including `NaN >= NaN`.
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
pub(crate) struct GtEqFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for GtEqFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        comparison::infer_types(Function::GtEq, args, target_type, inferred_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for GT_EQ");
        debug_assert!(target_type_set.contains(VarType::Bool));
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(GtEqFnCall {
                args: comparison::apply_types(args, context)?,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("GT_EQ", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        comparison::lower(
            &self.args,
            OrderedComparison::GreaterThanOrEqual,
            context,
            builder,
        )
    }
}

impl From<GtEqFnCall> for FnCallEnum {
    fn from(call: GtEqFnCall) -> Self {
        FnCallEnum::GtEq(call)
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
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: These expressions have no inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_requires_two_ordered_arguments() {
        let expression = deserialize("(GT_EQ left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        let left = inferred_types.get("left").unwrap();
        assert!(left.string && left.i64 && left.u64 && left.f64 && !left.boolean);

        let expression = deserialize("(GT_EQ 1i64 2i64 3i64)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::GtEq,
                expected: 2,
                ..
            })
        ));
    }

    #[test]
    fn test_ordering_equality_nan_and_null() {
        assert_eq!(eval(r#"(GT_EQ "same" "same")"#), Some(true));
        assert_eq!(eval(r#"(GT_EQ "alpha" "beta")"#), Some(false));
        assert_eq!(eval("(GT_EQ 0u64 -1i64)"), Some(true));
        assert_eq!(
            eval("(GT_EQ 9007199254740993u64 9007199254740992f64)"),
            Some(true)
        );
        assert_eq!(eval("(GT_EQ nanf64 nanf64)"), Some(false));
        assert_eq!(eval("(GT_EQ nanf64 0i64)"), Some(false));
        assert_eq!(eval("(GT_EQ 0i64 nanf64)"), Some(false));
        assert_eq!(eval("(GT_EQ none 1i64)"), None);
    }

    #[test]
    fn test_runtime_null_propagates() {
        let expression = deserialize("(GT_EQ left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some("a"), VariableValue::none()])
                    .as_bool()
            },
            None
        );
    }
}
