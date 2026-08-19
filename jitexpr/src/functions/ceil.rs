//! `CEIL` returns the least integer greater than or equal to one numeric argument.
//!
//! The result type is always `i64`. Signed integers are returned unchanged. Unsigned integers are
//! returned unchanged when they fit in `i64`; larger values return null. Floating-point inputs are
//! rounded toward positive infinity and converted to `i64`. Null, NaN, infinity, and any result
//! outside the `i64` range return null.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{FloatCC, InstBuilder, IntCC, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CeilFnCall {
    arg: Box<TypedExpr>,
}

impl FnCall for CeilFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::I64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Ceil,
                expected: target_type,
                got: InferredTypeSet::I64,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Ceil,
                expected: 1,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::NUMERICAL, inferred_types)?;
        Ok(InferredTypeSet::I64)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 1, "expected 1 arg for CEIL");
        debug_assert!(target_type_set.contains(VarType::I64));
        let arg = context.apply_types(&args[0], InferredTypeSet::NUMERICAL)?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type: VarType::I64,
            ast: TypedExprAst::from_call(CeilFnCall { arg: Box::new(arg) }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("CEIL", std::iter::once(self.arg.as_ref()), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        if return_type != VarType::I64 {
            return Err(CompileError::UnsupportedFunctionType {
                function: Function::Ceil,
                return_type,
            });
        }
        let arg = context.compile_expr(&self.arg, builder)?;
        let (value, value_is_valid) = match self.arg.return_type {
            VarType::I64 => {
                let valid = builder.ins().iconst(types::I8, 1);
                (arg.value, valid)
            }
            VarType::U64 => {
                let max = builder.ins().iconst(types::I64, i64::MAX);
                let valid = builder
                    .ins()
                    .icmp(IntCC::UnsignedLessThanOrEqual, arg.value, max);
                (arg.value, valid)
            }
            VarType::F64 => {
                let rounded = builder.ins().ceil(arg.value);
                let lower_bound = builder.ins().f64const(i64::MIN as f64);
                let upper_bound = builder.ins().f64const(-(i64::MIN as f64));
                let above_lower =
                    builder
                        .ins()
                        .fcmp(FloatCC::GreaterThanOrEqual, rounded, lower_bound);
                let below_upper = builder.ins().fcmp(FloatCC::LessThan, rounded, upper_bound);
                let valid = builder.ins().band(above_lower, below_upper);
                let value = builder.ins().fcvt_to_sint_sat(types::I64, rounded);
                (value, valid)
            }
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Ceil,
                    return_type: self.arg.return_type,
                });
            }
        };
        let is_present = builder.ins().band(arg.is_present, value_is_valid);
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<CeilFnCall> for FnCallEnum {
    fn from(call: CeilFnCall) -> Self {
        FnCallEnum::Ceil(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<i64> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        // SAFETY: The expression has no inputs and returns a nullable i64 value.
        unsafe { compiled.call(&[]).as_i64() }
    }

    #[test]
    fn test_signature_and_output_type() {
        let expression = deserialize("(CEIL value)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        assert_eq!(inferred.get("value"), Some(&InferredTypeSet::NUMERICAL));

        for expression in ["(CEIL)", "(CEIL 1i64 2i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Ceil,
                    expected: 1,
                    ..
                })
            ));
        }

        let expression = deserialize("(CEIL 1.2f64)").unwrap();
        assert_eq!(
            compile(&expression, &HashMap::new()).unwrap().result_type(),
            VarType::I64
        );
    }

    #[test]
    fn test_integer_identity_and_float_rounding() {
        assert_eq!(eval("(CEIL -9223372036854775808i64)"), Some(i64::MIN));
        assert_eq!(eval("(CEIL 9223372036854775807u64)"), Some(i64::MAX));
        assert_eq!(eval("(CEIL 9223372036854775808u64)"), None);
        assert_eq!(eval("(CEIL 1.2f64)"), Some(2));
        assert_eq!(eval("(CEIL -1.2f64)"), Some(-1));
        assert_eq!(eval("(CEIL -0f64)"), Some(0));
    }

    #[test]
    fn test_null_and_exceptional_float_results() {
        assert_eq!(eval("(CEIL none)"), None);
        assert_eq!(eval("(CEIL nanf64)"), None);
        assert_eq!(eval("(CEIL inff64)"), None);
        assert_eq!(eval("(CEIL -inff64)"), None);
        assert_eq!(eval("(CEIL 9223372036854775808f64)"), None);
        assert_eq!(eval("(CEIL -9223372036854775808f64)"), Some(i64::MIN));
    }

    #[test]
    fn test_runtime_null() {
        let expression = deserialize("(CEIL value)").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::F64)]))
            .unwrap()
            .context();

        // SAFETY: The compiled expression expects one nullable f64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(2.1f64)]).as_i64() },
            Some(3)
        );
        // SAFETY: The compiled expression expects one nullable f64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_i64() },
            None
        );
    }
}
