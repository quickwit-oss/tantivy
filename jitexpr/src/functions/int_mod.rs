//! `INT_MOD` computes production's floating-point `MOD` operation.
//!
//! Despite its calculated-field name, it accepts exactly two numeric arguments and always returns
//! `f64`. Integer inputs are converted to `f64` before the operation. The remainder is adjusted to
//! have the divisor's sign: for example `INT_MOD(-5, 3) = 1` and `INT_MOD(5, -3) = -1`. This is
//! dd-go's `modFloat` behavior, not Rust's unadjusted `%` result.
//!
//! A positive or negative zero divisor returns null. Null operands propagate. Other IEEE values
//! remain present: NaN produces NaN, while infinities follow the combination of floating remainder
//! and the sign adjustment. Production treats multivalued arithmetic inputs as null; arrays are
//! outside jitexpr's scalar model.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, FloatCC, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_float_mod";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IntModFnCall {
    args: Box<[TypedExpr]>,
}

impl FnCall for IntModFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::F64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::IntMod,
                expected: target_type,
                got: InferredTypeSet::F64,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::IntMod,
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
        assert_eq!(args.len(), 2, "expected 2 args for INT_MOD");
        debug_assert!(target_type_set.contains(VarType::F64));
        let args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::F64))
            .collect::<Result<Vec<_>, _>>()?;
        if args.iter().any(|arg| arg.return_type == VarType::None) {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type: VarType::F64,
            ast: TypedExprAst::from_call(IntModFnCall {
                args: args.into_boxed_slice(),
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
        debug_assert_eq!(return_type, VarType::F64);
        let value = context.compile_expr(&self.args[0], builder)?;
        let modulus = context.compile_expr(&self.args[1], builder)?;
        let call = builder.ins().call(
            context.native_functions().float_mod(),
            &[value.value, modulus.value],
        );
        let result = builder.inst_results(call)[0];
        let zero = builder.ins().f64const(0.0);
        let modulus_is_zero = builder.ins().fcmp(FloatCC::Equal, modulus.value, zero);
        let modulus_is_nonzero = builder.ins().bxor_imm_u(modulus_is_zero, 1);
        let both_present = builder.ins().band(value.is_present, modulus.is_present);
        let is_present = builder.ins().band(both_present, modulus_is_nonzero);
        Ok(LoweredValue {
            value: result,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

pub(super) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(SYMBOL, float_mod as *const u8);
}

pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature
        .params
        .extend(std::iter::repeat_n(AbiParam::new(types::F64), 2));
    signature.returns.push(AbiParam::new(types::F64));
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

extern "C" fn float_mod(value: f64, modulus: f64) -> f64 {
    if modulus == 0.0 {
        return 0.0;
    }
    let remainder = value % modulus;
    if (modulus > 0.0 && remainder < 0.0) || (modulus < 0.0 && remainder > 0.0) {
        remainder + modulus
    } else {
        remainder
    }
}

impl From<IntModFnCall> for FnCallEnum {
    fn from(call: IntModFnCall) -> Self {
        FnCallEnum::IntMod(call)
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
        // SAFETY: These expressions have no inputs and return nullable f64 values.
        unsafe { compiled.call(&[]).as_f64() }
    }

    #[test]
    fn test_requires_two_numeric_arguments_and_returns_float() {
        let expression = deserialize("(INT_MOD left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(
            inferred_types.get("left"),
            Some(&InferredTypeSet::NUMERICAL)
        );
        assert_eq!(
            inferred_types.get("right"),
            Some(&InferredTypeSet::NUMERICAL)
        );

        let expression = deserialize("(INT_MOD 1i64)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::IntMod,
                expected: 2,
                ..
            })
        ));
    }

    #[test]
    fn test_remainder_has_divisor_sign_and_output_is_float() {
        assert_eq!(eval("(INT_MOD 5i64 3i64)"), Some(2.0));
        assert_eq!(eval("(INT_MOD -5i64 3i64)"), Some(1.0));
        assert_eq!(eval("(INT_MOD 5i64 -3i64)"), Some(-1.0));
        assert_eq!(eval("(INT_MOD -5i64 -3i64)"), Some(-2.0));
        assert_eq!(eval("(INT_MOD 5.5f64 2f64)"), Some(1.5));
    }

    #[test]
    fn test_zero_null_nan_and_infinity_edges() {
        assert_eq!(eval("(INT_MOD 1f64 0f64)"), None);
        assert_eq!(eval("(INT_MOD 1f64 -0f64)"), None);
        assert!(eval("(INT_MOD nanf64 2f64)").unwrap().is_nan());
        assert!(eval("(INT_MOD 2f64 nanf64)").unwrap().is_nan());
        assert_eq!(eval("(INT_MOD -2f64 inff64)"), Some(f64::INFINITY));
        assert_eq!(eval("(INT_MOD none 2f64)"), None);
    }

    #[test]
    fn test_runtime_null_propagates() {
        let expression = deserialize("(INT_MOD value modulus)").unwrap();
        let variable_types = HashMap::from([("value", VarType::I64), ("modulus", VarType::F64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(-5i64), VariableValue::some(3.0f64)])
                    .as_f64()
            },
            Some(1.0)
        );
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(-5i64), VariableValue::none()])
                    .as_f64()
            },
            None
        );
    }
}
