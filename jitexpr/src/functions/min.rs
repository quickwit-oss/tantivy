//! `MIN` returns the least value among one or more numeric arguments.
//!
//! This is an expression function, not an aggregate. All arguments are coerced to one numeric type
//! and the result has that type. Any null argument makes the result null. The float implementation
//! starts at positive infinity and replaces it only on `<`, so NaNs are ignored; if every argument
//! is NaN, the result is positive infinity.

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
pub(crate) struct MinFnCall {
    args: Box<[TypedExpr]>,
}

impl FnCall for MinFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::AtLeast(1);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::NUMERICAL).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Min,
                expected: target_type,
                got: InferredTypeSet::NUMERICAL,
            });
        }
        if args.is_empty() {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Min,
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
                function: Function::Min,
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
            ast: TypedExprAst::from_call(MinFnCall {
                args: args.into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("MIN", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let mut value = match return_type {
            VarType::I64 => builder.ins().iconst(types::I64, i64::MAX),
            VarType::U64 => builder.ins().iconst(types::I64, -1),
            VarType::F64 => builder.ins().f64const(f64::INFINITY),
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Min,
                    return_type,
                });
            }
        };
        let mut is_present = builder.ins().iconst(types::I8, 1);
        for arg in &self.args {
            let arg = context.compile_expr(arg, builder)?;
            let is_less = match return_type {
                VarType::I64 => builder.ins().icmp(IntCC::SignedLessThan, arg.value, value),
                VarType::U64 => builder
                    .ins()
                    .icmp(IntCC::UnsignedLessThan, arg.value, value),
                VarType::F64 => builder.ins().fcmp(FloatCC::LessThan, arg.value, value),
                _ => unreachable!(),
            };
            value = builder.ins().select(is_less, arg.value, value);
            is_present = builder.ins().band(is_present, arg.is_present);
        }
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<MinFnCall> for FnCallEnum {
    fn from(call: MinFnCall) -> Self {
        FnCallEnum::Min(call)
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
        let expression = deserialize("(MIN a b c)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        for name in ["a", "b", "c"] {
            assert_eq!(inferred.get(name), Some(&InferredTypeSet::NUMERICAL));
        }
        let empty = deserialize("(MIN)").unwrap();
        assert!(matches!(
            infer_types(&empty),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Min,
                expected: 1,
                got: 0
            })
        ));

        let expression = deserialize("(MIN 7i64 -3i64 2i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_i64() }, Some(-3));
        let expression = deserialize("(MIN 7u64 3u64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_u64() }, Some(3));
    }

    #[test]
    fn test_float_nan_and_null_behavior() {
        let expression = deserialize("(MIN nanf64 3f64 -2f64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_f64() }, Some(-2.0));
        let expression = deserialize("(MIN nanf64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_f64() }, Some(f64::INFINITY));

        let expression = deserialize("(MIN left right)").unwrap();
        let mut compiled = compile(
            &expression,
            &HashMap::from([("left", VarType::I64), ("right", VarType::I64)]),
        )
        .unwrap()
        .context();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(1i64), VariableValue::none()])
                    .as_i64()
            },
            None
        );
    }
}
