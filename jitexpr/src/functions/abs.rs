//! `ABS` computes the absolute value of one numeric argument.
//!
//! It accepts exactly one number and preserves its selected input type. For signed integers,
//! negative values are negated with 64-bit wrapping, so `ABS(i64::MIN)` remains `i64::MIN`.
//! Unsigned values are unchanged. Floating-point values are negated only when `< 0.0`;
//! this preserves negative zero and leaves NaN (including its payload/sign bits) untouched.
//!
//! Null input returns null.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{FloatCC, InstBuilder, IntCC, types};

use super::add::{select_return_type, with_float_fallback};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AbsFnCall {
    arg: Box<TypedExpr>,
}

impl FnCall for AbsFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::Exactly(1);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::NUMERICAL).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Abs,
                expected: target_type,
                got: InferredTypeSet::NUMERICAL,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Abs,
                expected: 1,
                got: args.len(),
            });
        }
        let arg_types =
            crate::ast::infer_types_aux(&args[0], InferredTypeSet::NUMERICAL, inferred_types)?;
        let return_types = arg_types.intersect(target_type);
        if return_types.is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Abs,
                expected: target_type,
                got: arg_types,
            });
        }
        Ok(return_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        Self::ARG_COUNT.validate(args)?;
        let target_types = match &args[0] {
            UntypedExpr::Literal(literal) => {
                let declared = InferredTypeSet::singleton(literal.r#type());
                let constrained = declared.intersect(target_type_set);
                if constrained.is_none() {
                    InferredTypeSet::NUMERICAL.intersect(target_type_set)
                } else {
                    constrained
                }
            }
            UntypedExpr::Variable(variable) => context
                .variable_types()
                .get(variable.as_ref())
                .copied()
                .map(InferredTypeSet::singleton)
                .unwrap_or(InferredTypeSet::NONE)
                .intersect(target_type_set),
            _ => InferredTypeSet::NUMERICAL.intersect(target_type_set),
        };
        let return_type = select_return_type(with_float_fallback(target_types));
        let arg = context.apply_types(&args[0], InferredTypeSet::singleton(return_type))?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(AbsFnCall { arg: Box::new(arg) }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("ABS", std::iter::once(self.arg.as_ref()), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let arg = context.compile_expr(&self.arg, builder)?;
        let value = match return_type {
            VarType::I64 => {
                let is_negative = builder
                    .ins()
                    .icmp_imm_s(IntCC::SignedLessThan, arg.value, 0);
                let negated = builder.ins().ineg(arg.value);
                builder.ins().select(is_negative, negated, arg.value)
            }
            VarType::U64 => arg.value,
            VarType::F64 => {
                let zero = builder.ins().f64const(0.0);
                let is_negative = builder.ins().fcmp(FloatCC::LessThan, arg.value, zero);
                let negated = builder.ins().fneg(arg.value);
                builder.ins().select(is_negative, negated, arg.value)
            }
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Abs,
                    return_type,
                });
            }
        };
        Ok(LoweredValue {
            value,
            is_present: arg.is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<AbsFnCall> for FnCallEnum {
    fn from(call: AbsFnCall) -> Self {
        FnCallEnum::Abs(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_requires_one_numeric_argument() {
        let expression = deserialize("(ABS value)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(
            inferred_types.get("value"),
            Some(&InferredTypeSet::NUMERICAL)
        );

        for expression in ["(ABS)", "(ABS one two)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Abs,
                    expected: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_preserves_declared_numeric_types() {
        let cases = [
            ("(ABS -7i64)", VarType::I64),
            ("(ABS 7u64)", VarType::U64),
            ("(ABS -7f64)", VarType::F64),
        ];
        for (expression, expected_type) in cases {
            let expression = deserialize(expression).unwrap();
            let compiled = compile(&expression, &HashMap::new()).unwrap();
            assert_eq!(compiled.result_type(), expected_type);
        }
    }

    #[test]
    fn test_integer_edges_and_runtime_null() {
        let expression = deserialize("(ABS value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::I64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(-7i64)]).as_i64() },
            Some(7)
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(i64::MIN)]).as_i64() },
            Some(i64::MIN)
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_i64() },
            None
        );
    }

    #[test]
    fn test_float_preserves_negative_zero_and_nan_bits() {
        let expression = deserialize("(ABS value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::F64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();

        let negative_zero = unsafe {
            compiled
                .call(&[VariableValue::some(-0.0f64)])
                .as_f64()
                .unwrap()
        };
        assert_eq!(negative_zero.to_bits(), (-0.0f64).to_bits());

        let nan = f64::from_bits(0xfff8_0000_0000_0042);
        let output = unsafe { compiled.call(&[VariableValue::some(nan)]).as_f64().unwrap() };
        assert_eq!(output.to_bits(), nan.to_bits());
    }
}
