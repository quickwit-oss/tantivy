//! `MULTIPLY` multiplies two numeric expressions.
//!
//! It accepts exactly two numeric arguments. Both operands are coerced to one common numeric type
//! using the same rules as `ADD`: prefer a common `i64`, then `u64`, and fall back to `f64` when no
//! integer type represents both operands. Integer multiplication wraps at 64 bits; floating-point
//! multiplication follows IEEE-754, including NaN and infinity behavior.
//!
//! Null propagation is strict: if either operand is absent, the result is absent.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use super::add::{is_numerical, select_return_type, with_float_fallback};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MultiplyFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for MultiplyFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::NUMERICAL).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Multiply,
                expected: target_type,
                got: InferredTypeSet::NUMERICAL,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Multiply,
                expected: 2,
                got: args.len(),
            });
        }

        let mut return_types = InferredTypeSet::NUMERICAL;
        for arg in args {
            let arg_types =
                crate::ast::infer_types_aux(arg, InferredTypeSet::NUMERICAL, inferred_types)?;
            return_types = return_types.intersect(arg_types);
        }
        return_types = with_float_fallback(return_types);
        let constrained_return_types = return_types.intersect(target_type);
        if constrained_return_types.is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Multiply,
                expected: target_type,
                got: return_types,
            });
        }
        Ok(constrained_return_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for MULTIPLY");

        let mut return_types = InferredTypeSet::NUMERICAL.intersect(target_type_set);
        for arg in args {
            let arg_types = crate::ast::infer_type_with_variable_types(
                arg,
                InferredTypeSet::NUMERICAL,
                context.variable_types(),
            )?;
            return_types = return_types.intersect(arg_types);
        }
        let return_type = select_return_type(with_float_fallback(return_types));
        let typed_args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::singleton(return_type)))
            .collect::<Result<Vec<_>, _>>()?;
        if typed_args
            .iter()
            .any(|typed_arg| !is_numerical(typed_arg.return_type))
        {
            return Ok(TypedExpr::none());
        }

        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(MultiplyFnCall {
                args: typed_args.into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("MULTIPLY", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let left = context.compile_expr(&self.args[0], builder)?;
        let right = context.compile_expr(&self.args[1], builder)?;
        let value = match return_type {
            VarType::U64 | VarType::I64 => builder.ins().imul(left.value, right.value),
            VarType::F64 => builder.ins().fmul(left.value, right.value),
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Multiply,
                    return_type,
                });
            }
        };
        let is_present = builder.ins().band(left.is_present, right.is_present);
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<MultiplyFnCall> for FnCallEnum {
    fn from(call: MultiplyFnCall) -> Self {
        FnCallEnum::Multiply(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_infer_types_requires_two_numeric_arguments() {
        let expression = deserialize("(MULTIPLY left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(
            inferred_types.get("left"),
            Some(&InferredTypeSet::NUMERICAL)
        );
        assert_eq!(
            inferred_types.get("right"),
            Some(&InferredTypeSet::NUMERICAL)
        );

        for expression in ["(MULTIPLY 1i64)", "(MULTIPLY 1i64 2i64 3i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Multiply,
                    expected: 2,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_signed_unsigned_and_float_multiplication() {
        let expression = deserialize("(MULTIPLY -7i64 3i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: The expression has no inputs and returns i64.
        assert_eq!(unsafe { compiled.call(&[]).as_i64() }, Some(-21));

        let expression = deserialize("(MULTIPLY 9223372036854775808u64 2u64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        assert_eq!(compiled.result_type(), VarType::U64);
        // SAFETY: The expression has no inputs and returns u64; multiplication wraps.
        assert_eq!(unsafe { compiled.call(&[]).as_u64() }, Some(0));

        let expression = deserialize("(MULTIPLY 1.5f64 2f64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: The expression has no inputs and returns f64.
        assert_eq!(unsafe { compiled.call(&[]).as_f64() }, Some(3.0));
    }

    #[test]
    fn test_nan_is_preserved() {
        let expression = deserialize("(MULTIPLY nanf64 2f64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();

        // SAFETY: The expression has no inputs and returns f64.
        assert!(unsafe { compiled.call(&[]).as_f64() }.unwrap().is_nan());
    }

    #[test]
    fn test_runtime_null_propagation_and_integer_coercion() {
        let expression = deserialize("(MULTIPLY value 3i64)").unwrap();
        let variable_types = HashMap::from([("value", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(compiled.result_type(), VarType::U64);

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(4u64)]).as_u64() },
            Some(12)
        );
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_u64() },
            None
        );
    }

    #[test]
    fn test_compile_time_none_propagates() {
        let expression = deserialize("(MULTIPLY none 2i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();

        assert_eq!(compiled.result_type(), VarType::None);
        // SAFETY: The expression has no inputs and returns an absent value.
        assert_eq!(unsafe { compiled.call(&[]).as_i64() }, None);
    }
}
