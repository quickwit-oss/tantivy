//! `IS_NOT_NULL` observes nullability without propagating it.
//!
//! It accepts exactly one expression of any supported type and always returns a
//! present boolean: `false` when its argument is absent and `true` when it is
//! present. The payload is irrelevant, so present values such as `false`, zero,
//! `NaN`, and the empty string all return `true`.
//!
//! This matches dd-go's `NOT_NULL` unary runtime operator
//! (`interpreter/expression_unary.go` and `arrayNOTNULL`): unlike ordinary
//! expressions, it clears output nullability. dd-go additionally applies
//! context-sensitive type selection to bare columns before this runtime
//! operation; that query-level column resolution is outside this local
//! expression node.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IsNotNullFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for IsNotNullFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::IsNotNull,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::IsNotNull,
                expected: 1,
                got: args.len(),
            });
        }

        crate::ast::infer_types_aux(&args[0], InferredTypeSet::ALL, inferred_types)?;
        Ok(InferredTypeSet::BOOLEAN)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 1, "Expected 1 arg for IS_NOT_NULL");
        debug_assert!(target_type_set.contains(VarType::Bool));

        let arg = context.apply_types(&args[0], InferredTypeSet::ALL)?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(IsNotNullFnCall {
                args: vec![arg].into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let arg = context.compile_expr(&self.args[0], builder)?;
        let is_present = builder.ins().iconst(types::I8, 1);
        Ok(LoweredValue {
            value: arg.is_present,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<IsNotNullFnCall> for FnCallEnum {
    fn from(call: IsNotNullFnCall) -> Self {
        FnCallEnum::IsNotNull(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types, infer_types_with_target};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval_without_args(expression: &str) -> (bool, Option<bool>) {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        assert!(compiled.inputs.is_empty());
        // SAFETY: The compiled expression has no inputs.
        let output = unsafe { compiled.call(&[]) };
        (unsafe { output.primitive.is_present }, unsafe {
            output.as_bool()
        })
    }

    fn eval_variable(var_type: VarType, input: VariableValue<'_>) -> (bool, Option<bool>) {
        let expression = deserialize("(IS_NOT_NULL value)").unwrap();
        let variable_types = HashMap::from([("value", var_type)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        // SAFETY: `input` is constructed with the union member matching `var_type`
        // at each call site, or is absent and therefore has no active payload.
        let output = unsafe { compiled.call(&[input]) };
        (unsafe { output.primitive.is_present }, unsafe {
            output.as_bool()
        })
    }

    #[test]
    fn test_infer_types_accepts_any_argument_and_returns_bool() {
        let expression = deserialize("(IS_NOT_NULL value)").unwrap();

        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::ALL));
        assert!(matches!(
            infer_types_with_target(&expression, InferredTypeSet::F64),
            Err(TypeError::WrongFunctionReturnType {
                function: Function::IsNotNull,
                got: InferredTypeSet::BOOLEAN,
                ..
            })
        ));
    }

    #[test]
    fn test_rejects_no_arguments() {
        let expression = deserialize("(IS_NOT_NULL)").unwrap();

        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::IsNotNull,
                expected: 1,
                got: 0,
            })
        ));
    }

    #[test]
    fn test_rejects_more_than_one_argument() {
        let expression = deserialize("(IS_NOT_NULL value other)").unwrap();

        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::IsNotNull,
                expected: 1,
                got: 2,
            })
        ));
    }

    #[test]
    fn test_none_literal_and_missing_variable_are_false_but_present() {
        let none = eval_without_args("(IS_NOT_NULL none)");
        assert!(none.0);
        assert_eq!(none.1, Some(false));

        let missing = eval_without_args("(IS_NOT_NULL missing)");
        assert!(missing.0);
        assert_eq!(missing.1, Some(false));
    }

    #[test]
    fn test_absent_runtime_values_of_every_type_are_false() {
        for var_type in [
            VarType::Bool,
            VarType::F64,
            VarType::U64,
            VarType::I64,
            VarType::Str,
        ] {
            let output = eval_variable(var_type, VariableValue::none());
            assert!(output.0, "type: {var_type:?}");
            assert_eq!(output.1, Some(false), "type: {var_type:?}");
        }
    }

    #[test]
    fn test_present_edge_values_are_true() {
        for (var_type, input) in [
            (VarType::Bool, VariableValue::some(false)),
            (VarType::F64, VariableValue::some(f64::NAN)),
            (VarType::U64, VariableValue::some(0u64)),
            (VarType::I64, VariableValue::some(i64::MIN)),
        ] {
            let output = eval_variable(var_type, input);
            assert_eq!(output.1, Some(true), "type: {var_type:?}");
        }

        let output = eval_variable(VarType::Str, VariableValue::some(""));
        assert_eq!(output.1, Some(true));
    }

    #[test]
    fn test_observes_nested_expression_nullability() {
        assert_eq!(
            eval_without_args(r#"(IS_NOT_NULL (REGEXP_EXTRACT "b" "(a*)b" 1u64))"#).1,
            Some(true)
        );
        assert_eq!(
            eval_without_args(r#"(IS_NOT_NULL (REGEXP_EXTRACT "b" "(a+)" 1u64))"#).1,
            Some(false)
        );
    }
}
