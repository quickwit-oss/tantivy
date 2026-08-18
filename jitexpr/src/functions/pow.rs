//! `POW` raises a scalar numeric base to a scalar numeric exponent.
//!
//! It accepts exactly two numeric arguments. Both are converted to `f64`, and the result is always
//! `f64`. A negative base with a non-integral exponent represents a complex result and is returned
//! as null, matching dd-go's explicit `powIsNaN` check. Null operands propagate.
//!
//! Other IEEE-754 results remain present: overflow may produce infinity, and a NaN base may
//! produce NaN. Production returns null for multivalued arithmetic inputs; arrays are outside
//! jitexpr's scalar type model.

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

const SYMBOL: &str = "jitexpr_float_pow";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PowFnCall {
    args: Box<[TypedExpr]>,
}

impl FnCall for PowFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::F64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Pow,
                expected: target_type,
                got: InferredTypeSet::F64,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Pow,
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
        assert_eq!(args.len(), 2, "expected 2 args for POW");
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
            ast: TypedExprAst::from_call(PowFnCall {
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
        let base = context.compile_expr(&self.args[0], builder)?;
        let exponent = context.compile_expr(&self.args[1], builder)?;
        let call = builder.ins().call(
            context.native_functions().float_pow(),
            &[base.value, exponent.value],
        );
        let result = builder.inst_results(call)[0];

        let zero = builder.ins().f64const(0.0);
        let base_is_negative = builder.ins().fcmp(FloatCC::LessThan, base.value, zero);
        let result_is_nan = builder.ins().fcmp(FloatCC::Unordered, result, result);
        let complex_result = builder.ins().band(base_is_negative, result_is_nan);
        let is_real = builder.ins().bxor_imm_u(complex_result, 1);
        let both_present = builder.ins().band(base.is_present, exponent.is_present);
        let is_present = builder.ins().band(both_present, is_real);

        Ok(LoweredValue {
            value: result,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

pub(super) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(SYMBOL, float_pow as *const u8);
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

extern "C" fn float_pow(base: f64, exponent: f64) -> f64 {
    base.powf(exponent)
}

impl From<PowFnCall> for FnCallEnum {
    fn from(call: PowFnCall) -> Self {
        FnCallEnum::Pow(call)
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
    fn test_signature_and_output_type() {
        let expression = deserialize("(POW base exponent)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        assert_eq!(inferred.get("base"), Some(&InferredTypeSet::NUMERICAL));
        assert_eq!(inferred.get("exponent"), Some(&InferredTypeSet::NUMERICAL));

        for expression in ["(POW 2i64)", "(POW 2i64 3i64 4i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Pow,
                    expected: 2,
                    ..
                })
            ));
        }
        let expression = deserialize("(POW 2i64 3i64)").unwrap();
        assert_eq!(
            compile(&expression, &HashMap::new()).unwrap().result_type(),
            VarType::F64
        );
    }

    #[test]
    fn test_numeric_and_ieee_edges() {
        assert_eq!(eval("(POW 2i64 3i64)"), Some(8.0));
        assert_eq!(eval("(POW -2f64 3f64)"), Some(-8.0));
        assert_eq!(eval("(POW -2f64 0.5f64)"), None);
        assert_eq!(eval("(POW none 2f64)"), None);
        assert!(eval("(POW nanf64 2f64)").unwrap().is_nan());
        assert!(eval("(POW 1e308f64 2f64)").unwrap().is_infinite());
    }

    #[test]
    fn test_runtime_null_propagates() {
        let expression = deserialize("(POW base exponent)").unwrap();
        let variable_types = HashMap::from([("base", VarType::I64), ("exponent", VarType::F64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(3i64), VariableValue::some(2.0f64)])
                    .as_f64()
            },
            Some(9.0)
        );
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(3i64), VariableValue::none()])
                    .as_f64()
            },
            None
        );
    }
}
