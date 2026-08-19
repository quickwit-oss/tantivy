//! `SQRT` computes the square root of one numeric argument.
//!
//! The argument is coerced to `f64`, and the result is always `f64`. Null input returns null. A
//! NaN result also returns null, covering NaN input and negative values other than negative zero.
//! Positive infinity and signed zero retain their IEEE-754 behavior.

use std::collections::HashMap;

use cranelift::prelude::{FloatCC, FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqrtFnCall {
    arg: Box<TypedExpr>,
}

impl FnCall for SqrtFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::F64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Sqrt,
                expected: target_type,
                got: InferredTypeSet::F64,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Sqrt,
                expected: 1,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::NUMERICAL, inferred_types)?;
        Ok(InferredTypeSet::F64)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 1, "expected 1 arg for SQRT");
        debug_assert!(target_type_set.contains(VarType::F64));
        let arg = context.apply_types(&args[0], InferredTypeSet::F64)?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type: VarType::F64,
            ast: TypedExprAst::from_call(SqrtFnCall { arg: Box::new(arg) }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        if return_type != VarType::F64 {
            return Err(CompileError::UnsupportedFunctionType {
                function: Function::Sqrt,
                return_type,
            });
        }
        let arg = context.compile_expr(&self.arg, builder)?;
        let value = builder.ins().sqrt(arg.value);
        let is_nan = builder.ins().fcmp(FloatCC::Unordered, value, value);
        let is_not_nan = builder.ins().bxor_imm_u(is_nan, 1);
        let is_present = builder.ins().band(arg.is_present, is_not_nan);
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<SqrtFnCall> for FnCallEnum {
    fn from(call: SqrtFnCall) -> Self {
        FnCallEnum::Sqrt(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<f64> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: The expression has no inputs and returns a nullable f64 value.
        unsafe { compiled.call(&[]).as_f64() }
    }

    #[test]
    fn test_signature_and_output_type() {
        let expression = deserialize("(SQRT value)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        assert_eq!(inferred.get("value"), Some(&InferredTypeSet::NUMERICAL));

        for expression in ["(SQRT)", "(SQRT 1i64 2i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Sqrt,
                    expected: 1,
                    ..
                })
            ));
        }

        let expression = deserialize("(SQRT 9i64)").unwrap();
        assert_eq!(
            compile(&expression, &HashMap::new()).unwrap().result_type(),
            VarType::F64
        );
    }

    #[test]
    fn test_numeric_inputs_are_converted_to_float() {
        assert_eq!(eval("(SQRT 9i64)"), Some(3.0));
        assert_eq!(eval("(SQRT 2.25f64)"), Some(1.5));
        assert_eq!(eval("(SQRT 2u64)"), Some(2.0f64.sqrt()));
    }

    #[test]
    fn test_null_nan_and_ieee_edges() {
        assert_eq!(eval("(SQRT none)"), None);
        assert_eq!(eval("(SQRT -1i64)"), None);
        assert_eq!(eval("(SQRT nanf64)"), None);
        assert_eq!(eval("(SQRT inff64)"), Some(f64::INFINITY));

        let negative_zero = eval("(SQRT -0f64)").unwrap();
        assert_eq!(negative_zero.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn test_runtime_null_and_negative_input() {
        let expression = deserialize("(SQRT value)").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::I64)])).unwrap();

        // SAFETY: The compiled expression expects one nullable i64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(16i64)]).as_f64() },
            Some(4.0)
        );
        // SAFETY: The compiled expression expects one nullable i64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(-16i64)]).as_f64() },
            None
        );
        // SAFETY: The compiled expression expects one nullable i64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_f64() },
            None
        );
    }
}
