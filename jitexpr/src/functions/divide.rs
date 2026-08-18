//! `DIVIDE` performs floating-point division.
//!
//! It accepts exactly two numeric arguments and always coerces both to `f64`, even when both are
//! integers. This avoids the surprising integer-division behavior explicitly called out by the
//! dd-go type checker (`BinaryExpression_DIVIDE` in `expression_type_checker.go`). jitexpr's
//! scalar API accepts its native numeric types; the reader's additional string-parsing coercion is
//! outside the current expression type model.
//!
//! The result is absent if either operand is absent or if the divisor is positive or negative
//! zero. Division by zero therefore yields NULL rather than infinity or NaN. Otherwise IEEE-754
//! behavior applies, including propagation of NaN and infinities. This matches the zero guard and
//! null propagation in dd-go's `arithmeticDIVIDE` kernels (`vector_generated.go`).

use std::collections::HashMap;

use cranelift::prelude::{FloatCC, FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DivideFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for DivideFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::F64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Divide,
                expected: target_type,
                got: InferredTypeSet::F64,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Divide,
                expected: 2,
                got: args.len(),
            });
        }

        for arg in args {
            crate::ast::infer_types_aux(arg, InferredTypeSet::NUMERICAL, inferred_types)?;
        }
        Ok(InferredTypeSet::F64)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for DIVIDE");
        debug_assert!(target_type_set.contains(VarType::F64));

        let typed_args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::F64))
            .collect::<Result<Vec<_>, _>>()?;
        if typed_args
            .iter()
            .any(|typed_arg| typed_arg.return_type == VarType::None)
        {
            return Ok(TypedExpr::none());
        }

        Ok(TypedExpr {
            return_type: VarType::F64,
            ast: TypedExprAst::from_call(DivideFnCall {
                args: typed_args.into_boxed_slice(),
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
        if return_type != VarType::F64 {
            return Err(CompileError::UnsupportedFunctionType {
                function: Function::Divide,
                return_type,
            });
        }

        let dividend = context.compile_expr(&self.args[0], builder)?;
        let divisor = context.compile_expr(&self.args[1], builder)?;
        let value = builder.ins().fdiv(dividend.value, divisor.value);
        let zero = builder.ins().f64const(0.0);
        let divisor_is_zero = builder.ins().fcmp(FloatCC::Equal, divisor.value, zero);
        let divisor_is_nonzero = builder.ins().bxor_imm_u(divisor_is_zero, 1);
        let both_present = builder.ins().band(dividend.is_present, divisor.is_present);
        let is_present = builder.ins().band(both_present, divisor_is_nonzero);
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<DivideFnCall> for FnCallEnum {
    fn from(call: DivideFnCall) -> Self {
        FnCallEnum::Divide(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_infer_types_requires_two_numeric_arguments_and_returns_float() {
        let expression = deserialize("(DIVIDE left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(
            inferred_types.get("left"),
            Some(&InferredTypeSet::NUMERICAL)
        );
        assert_eq!(
            inferred_types.get("right"),
            Some(&InferredTypeSet::NUMERICAL)
        );

        for expression in ["(DIVIDE 1i64)", "(DIVIDE 1i64 2i64 3i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Divide,
                    expected: 2,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_integer_inputs_use_floating_point_division() {
        let expression = deserialize("(DIVIDE 5i64 2i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();

        assert_eq!(compiled.result_type(), VarType::F64);
        // SAFETY: The expression has no inputs and returns f64.
        assert_eq!(unsafe { compiled.call(&[]).as_f64() }, Some(2.5));
    }

    #[test]
    fn test_positive_and_negative_zero_divisors_return_none() {
        for expression in ["(DIVIDE 1f64 0f64)", "(DIVIDE 1f64 -0f64)"] {
            let expression = deserialize(expression).unwrap();
            let mut compiled = compile(&expression, &HashMap::new()).unwrap();
            // SAFETY: The expression has no inputs and returns nullable f64.
            assert_eq!(unsafe { compiled.call(&[]).as_f64() }, None);
        }
    }

    #[test]
    fn test_nan_divisor_remains_present() {
        let expression = deserialize("(DIVIDE 1f64 nanf64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();

        // SAFETY: The expression has no inputs and returns f64.
        assert!(unsafe { compiled.call(&[]).as_f64() }.unwrap().is_nan());
    }

    #[test]
    fn test_runtime_null_propagation() {
        let expression = deserialize("(DIVIDE left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::I64), ("right", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(9i64), VariableValue::some(2u64)])
                    .as_f64()
            },
            Some(4.5)
        );
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::some(2u64)])
                    .as_f64()
            },
            None
        );
    }
}
