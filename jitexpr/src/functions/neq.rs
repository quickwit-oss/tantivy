//! `NEQ` tests two values for inequality by composing `NOT(EQ(...))` semantics.
//!
//! It accepts exactly two operands of any supported type. Values of the same type compare normally,
//! numeric types compare across `i64`, `u64`, and `f64`, and unrelated present types are unequal.
//! NaN is unequal to every value, including itself.
//!
//! This is deliberately not a strict-null primitive. `EQ` first produces null when either operand
//! is null; `NOT` then converts that null result to present `true`. Consequently `NEQ(null,
//! value)`, `NEQ(value, null)`, and `NEQ(null, null)` are all present and true.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{InstBuilder, IntCC, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{EqFnCall, FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NeqFnCall {
    eq: EqFnCall,
}

impl FnCall for NeqFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Neq,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Neq,
                expected: 2,
                got: args.len(),
            });
        }
        for arg in args {
            crate::ast::infer_types_aux(arg, InferredTypeSet::ALL, inferred_types)?;
        }
        Ok(InferredTypeSet::BOOLEAN)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for NEQ");
        debug_assert!(target_type_set.contains(VarType::Bool));
        let typed_args = args
            .iter()
            .map(|arg| match arg {
                UntypedExpr::Literal(literal) => {
                    context.apply_types(arg, InferredTypeSet::singleton(literal.r#type()))
                }
                _ => context.apply_types(arg, InferredTypeSet::ALL),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(NeqFnCall {
                eq: EqFnCall {
                    args: typed_args.into_boxed_slice(),
                },
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        self.eq.args_mut()
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let equal = self.eq.emit_cranelift_ir(VarType::Bool, context, builder)?;
        let value = builder.ins().icmp_imm_u(IntCC::Equal, equal.value, 0);
        Ok(LoweredValue {
            value,
            is_present: builder.ins().iconst(types::I8, 1),
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<NeqFnCall> for FnCallEnum {
    fn from(call: NeqFnCall) -> Self {
        FnCallEnum::Neq(call)
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
        // SAFETY: These expressions have no inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_requires_two_arguments_of_any_type() {
        let expression = deserialize("(NEQ left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("left"), Some(&InferredTypeSet::ALL));
        assert_eq!(inferred_types.get("right"), Some(&InferredTypeSet::ALL));

        let expression = deserialize("(NEQ 1i64)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Neq,
                expected: 2,
                ..
            })
        ));
    }

    #[test]
    fn test_present_values_and_numeric_cross_types() {
        assert_eq!(eval("(NEQ 1i64 1u64)"), Some(false));
        assert_eq!(eval("(NEQ 1i64 2f64)"), Some(true));
        assert_eq!(eval(r#"(NEQ "same" "same")"#), Some(false));
        assert_eq!(eval(r#"(NEQ "1" 1i64)"#), Some(true));
        assert_eq!(eval("(NEQ nanf64 nanf64)"), Some(true));
    }

    #[test]
    fn test_null_is_converted_to_present_true() {
        assert_eq!(eval("(NEQ none 1i64)"), Some(true));
        assert_eq!(eval("(NEQ 1i64 none)"), Some(true));
        assert_eq!(eval("(NEQ none none)"), Some(true));

        let expression = deserialize("(NEQ left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::some("value")])
                    .as_bool()
            },
            Some(true)
        );
    }
}
