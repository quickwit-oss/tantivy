//! `IS_NULL` observes absence without propagating it.
//!
//! It accepts exactly one expression of any supported type and always returns a present boolean:
//! `true` when its argument is absent and `false` when it is present. Payload values such as
//! `false`, zero, `NaN`, and the empty string are present and therefore return `false`.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IsNullFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for IsNullFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::IsNull,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::IsNull,
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
        assert_eq!(args.len(), 1, "expected 1 arg for IS_NULL");
        debug_assert!(target_type_set.contains(VarType::Bool));

        let arg = context.apply_types(&args[0], InferredTypeSet::ALL)?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(IsNullFnCall {
                args: vec![arg].into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("IS_NULL", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let arg = context.compile_expr(&self.args[0], builder)?;
        let value = builder.ins().bxor_imm_u(arg.is_present, 1);
        Ok(LoweredValue {
            value,
            is_present: builder.ins().iconst(types::I8, 1),
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<IsNullFnCall> for FnCallEnum {
    fn from(call: IsNullFnCall) -> Self {
        FnCallEnum::IsNull(call)
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
        // SAFETY: These expressions have no runtime inputs and return booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_infer_types_accepts_any_single_argument() {
        let expression = deserialize("(IS_NULL value)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::ALL));
    }

    #[test]
    fn test_rejects_wrong_arity() {
        for expression in ["(IS_NULL)", "(IS_NULL value other)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::IsNull,
                    expected: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_absent_values_are_true() {
        assert_eq!(eval("(IS_NULL none)"), Some(true));
        assert_eq!(eval("(IS_NULL missing)"), Some(true));
        assert_eq!(
            eval(r#"(IS_NULL (REGEXP_EXTRACT "b" "(a+)" 1u64))"#),
            Some(true)
        );
    }

    #[test]
    fn test_present_edge_values_are_false() {
        for expression in [
            "(IS_NULL false)",
            "(IS_NULL 0i64)",
            "(IS_NULL nanf64)",
            r#"(IS_NULL "")"#,
            r#"(IS_NULL (REGEXP_EXTRACT "b" "(a*)b" 1u64))"#,
        ] {
            assert_eq!(eval(expression), Some(false), "expression: {expression}");
        }
    }

    #[test]
    fn test_runtime_absent_variable_is_true_and_present_variable_is_false() {
        let expression = deserialize("(IS_NULL value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::I64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_bool() },
            Some(true)
        );
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(0i64)]).as_bool() },
            Some(false)
        );
    }
}
