//! `LT_EQ` tests whether one value is less than or equal to another.
//!
//! It accepts exactly two operands. Ordered operands are strings or numbers; booleans are rejected.
//! Strings use lexicographic UTF-8 ordering. Numeric comparisons support `i64`, `u64`, and `f64`
//! combinations without converting large integers through a lossy `f64`. IEEE unordered
//! comparisons involving NaN return `false`, including `NaN <= NaN`.
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
pub(crate) struct LtEqFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for LtEqFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::Exactly(2);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        comparison::infer_types(Function::LtEq, args, target_type, inferred_types)
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
            ast: TypedExprAst::from_call(LtEqFnCall {
                args: comparison::apply_types(args, context)?,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("LT_EQ", self.args.iter(), formatter)
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
            OrderedComparison::LessThanOrEqual,
            context,
            builder,
        )
    }
}

impl From<LtEqFnCall> for FnCallEnum {
    fn from(call: LtEqFnCall) -> Self {
        FnCallEnum::LtEq(call)
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
        let expression = deserialize("(LT_EQ left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        let left = inferred_types.get("left").unwrap();
        assert!(left.string && left.i64 && left.u64 && left.f64 && !left.boolean);

        let expression = deserialize("(LT_EQ)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::LtEq,
                expected: 2,
                ..
            })
        ));
    }

    #[test]
    fn test_ordering_equality_nan_and_null() {
        assert_eq!(eval(r#"(LT_EQ "same" "same")"#), Some(true));
        assert_eq!(eval(r#"(LT_EQ "beta" "alpha")"#), Some(false));
        assert_eq!(eval("(LT_EQ -1i64 0u64)"), Some(true));
        assert_eq!(
            eval("(LT_EQ 9007199254740992f64 9007199254740993u64)"),
            Some(true)
        );
        assert_eq!(eval("(LT_EQ nanf64 nanf64)"), Some(false));
        assert_eq!(eval("(LT_EQ nanf64 0u64)"), Some(false));
        assert_eq!(eval("(LT_EQ 0u64 nanf64)"), Some(false));
        assert_eq!(eval("(LT_EQ none 1i64)"), None);
    }

    #[test]
    fn test_runtime_null_propagates() {
        let expression = deserialize("(LT_EQ left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::F64), ("right", VarType::I64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(1.0f64), VariableValue::none()])
                    .as_bool()
            },
            None
        );
    }
}
