//! `MAX` returns the greatest value among one or more numeric arguments.
//!
//! This is an expression function, not an aggregate. All arguments are coerced to one numeric type
//! and the result has that type. Any null argument makes the result null. The float implementation
//! starts at negative infinity and replaces it only on `>`, so NaNs are ignored; if every argument
//! is NaN, the result is negative infinity.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{FloatCC, InstBuilder, IntCC, types};

use super::add::{is_numerical, select_return_type, with_float_fallback};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MaxFnCall {
    args: Box<[TypedExpr]>,
}

impl FnCall for MaxFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::AtLeast(1);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::NUMERICAL).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Max,
                expected: target_type,
                got: InferredTypeSet::NUMERICAL,
            });
        }
        if args.is_empty() {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Max,
                expected: 1,
                got: 0,
            });
        }
        let mut return_types = InferredTypeSet::NUMERICAL;
        for arg in args {
            return_types = return_types.intersect(crate::ast::infer_types_aux(
                arg,
                InferredTypeSet::NUMERICAL,
                inferred_types,
            )?);
        }
        let result = with_float_fallback(return_types).intersect(target_type);
        if result.is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Max,
                expected: target_type,
                got: return_types,
            });
        }
        Ok(result)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        Self::ARG_COUNT.validate(args)?;
        let mut return_types = InferredTypeSet::NUMERICAL.intersect(target_type_set);
        for arg in args {
            return_types = return_types.intersect(crate::ast::infer_type_with_variable_types(
                arg,
                InferredTypeSet::NUMERICAL,
                context.variable_types(),
            )?);
        }
        let return_type = select_return_type(with_float_fallback(return_types));
        let args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::singleton(return_type)))
            .collect::<Result<Vec<_>, _>>()?;
        if args.iter().any(|arg| !is_numerical(arg.return_type)) {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(MaxFnCall {
                args: args.into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("MAX", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let mut value = match return_type {
            VarType::I64 => builder.ins().iconst(types::I64, i64::MIN),
            VarType::U64 => builder.ins().iconst(types::I64, 0),
            VarType::F64 => builder.ins().f64const(f64::NEG_INFINITY),
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Max,
                    return_type,
                });
            }
        };
        let mut is_present = builder.ins().iconst(types::I8, 1);
        for arg in &self.args {
            let arg = context.compile_expr(arg, builder)?;
            let is_greater = match return_type {
                VarType::I64 => builder
                    .ins()
                    .icmp(IntCC::SignedGreaterThan, arg.value, value),
                VarType::U64 => builder
                    .ins()
                    .icmp(IntCC::UnsignedGreaterThan, arg.value, value),
                VarType::F64 => builder.ins().fcmp(FloatCC::GreaterThan, arg.value, value),
                _ => unreachable!(),
            };
            value = builder.ins().select(is_greater, arg.value, value);
            is_present = builder.ins().band(is_present, arg.is_present);
        }
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<MaxFnCall> for FnCallEnum {
    fn from(call: MaxFnCall) -> Self {
        FnCallEnum::Max(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_signature_and_variadic_values() {
        let expression = deserialize("(MAX a b c)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        for name in ["a", "b", "c"] {
            assert_eq!(inferred.get(name), Some(&InferredTypeSet::NUMERICAL));
        }
        let empty = deserialize("(MAX)").unwrap();
        assert!(matches!(
            infer_types(&empty),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Max,
                expected: 1,
                got: 0
            })
        ));
        let expression = deserialize("(MAX 7i64 -3i64 12i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_i64() }, Some(12));
        let expression = deserialize("(MAX 7u64 13u64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_u64() }, Some(13));
    }

    #[test]
    fn test_float_nan_and_null_behavior() {
        let expression = deserialize("(MAX nanf64 3f64 -2f64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_f64() }, Some(3.0));
        let expression = deserialize("(MAX nanf64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(
            unsafe { compiled.call(&[]).as_f64() },
            Some(f64::NEG_INFINITY)
        );
        let expression = deserialize("(MAX left right)").unwrap();
        let mut compiled = compile(
            &expression,
            &HashMap::from([("left", VarType::F64), ("right", VarType::F64)]),
        )
        .unwrap()
        .context();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(1.0f64), VariableValue::none()])
                    .as_f64()
            },
            None
        );
    }
}
