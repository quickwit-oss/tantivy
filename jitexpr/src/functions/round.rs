//! `ROUND(value, precision)` rounds one numeric argument to a decimal precision.
//!
//! Precision is an optional integer constant and defaults to zero. Halfway values are rounded away
//! from zero. A positive precision keeps digits to the right of the decimal point and returns
//! `f64`; a zero or negative precision rounds to units, tens, hundreds, and so on and returns
//! `i64`. Integer inputs use exact integer arithmetic whenever the result type is `i64`.
//!
//! Null input or null precision returns null. An `i64` result is also null when the rounded value
//! is NaN, infinite, or outside the `i64` range. An `f64` result preserves NaN and infinity. Very
//! large positive precisions leave finite values unchanged, while very large negative precisions
//! round finite values to zero.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder, IntCC};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const ROUND_FLOAT_SYMBOL: &str = "jitexpr_round_float";
const ROUND_INT_TO_I64_SYMBOL: &str = "jitexpr_round_int_to_i64";
const ROUND_FLOAT_TO_I64_SYMBOL: &str = "jitexpr_round_float_to_i64";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RoundFnCall {
    arg: Box<TypedExpr>,
    precision: i64,
}

fn constant_precision(expression: Option<&UntypedExpr>) -> Option<i64> {
    let Some(expression) = expression else {
        return Some(0);
    };
    let UntypedExpr::Literal(literal) = expression else {
        panic!("ROUND precision must be constant");
    };
    match literal {
        Literal::I64(value) => Some(*value),
        Literal::U64(value) => i64::try_from(*value).ok(),
        Literal::F64(value)
            if value.is_finite()
                && value.fract() == 0.0
                && *value >= i64::MIN as f64
                && *value < -(i64::MIN as f64) =>
        {
            Some(*value as i64)
        }
        Literal::None => None,
        Literal::F64(_) | Literal::Bool(_) | Literal::String(_) => {
            unreachable!("type inference constrains ROUND precision to an integer")
        }
    }
}

fn return_type_for_precision(precision: i64) -> VarType {
    if precision > 0 {
        VarType::F64
    } else {
        VarType::I64
    }
}

impl FnCall for RoundFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if !(1..=2).contains(&args.len()) {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Round,
                expected: 2,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::NUMERICAL, inferred_types)?;
        if let Some(precision) = args.get(1) {
            crate::ast::infer_types_aux(precision, InferredTypeSet::I64, inferred_types)?;
        }
        let precision = constant_precision(args.get(1)).unwrap_or(0);
        let return_types = InferredTypeSet::singleton(return_type_for_precision(precision));
        if target_type.intersect(return_types).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Round,
                expected: target_type,
                got: return_types,
            });
        }
        Ok(return_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert!(
            (1..=2).contains(&args.len()),
            "expected 1 or 2 args for ROUND"
        );
        let Some(precision) = constant_precision(args.get(1)) else {
            return Ok(TypedExpr::none());
        };
        let return_type = return_type_for_precision(precision);
        debug_assert!(target_type_set.contains(return_type));
        let arg = context.apply_types(&args[0], InferredTypeSet::NUMERICAL)?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(RoundFnCall {
                arg: Box::new(arg),
                precision,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "ROUND {} {}i64", self.arg, self.precision)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let arg = context.compile_expr(&self.arg, builder)?;
        let precision = builder.ins().iconst(types::I64, self.precision);
        let string_len = builder.ins().iconst(types::I64, 0);

        if return_type == VarType::F64 {
            let float_value = match self.arg.return_type {
                VarType::I64 => builder.ins().fcvt_from_sint(types::F64, arg.value),
                VarType::U64 => builder.ins().fcvt_from_uint(types::F64, arg.value),
                VarType::F64 => arg.value,
                _ => {
                    return Err(CompileError::UnsupportedFunctionType {
                        function: Function::Round,
                        return_type: self.arg.return_type,
                    });
                }
            };
            let call = builder.ins().call(
                context.native_functions().round_float(),
                &[float_value, precision],
            );
            return Ok(LoweredValue {
                value: builder.inst_results(call)[0],
                is_present: arg.is_present,
                string_len,
            });
        }

        if return_type != VarType::I64 {
            return Err(CompileError::UnsupportedFunctionType {
                function: Function::Round,
                return_type,
            });
        }
        let call = match self.arg.return_type {
            VarType::I64 | VarType::U64 => {
                let is_signed = builder
                    .ins()
                    .iconst(types::I64, i64::from(self.arg.return_type == VarType::I64));
                builder.ins().call(
                    context.native_functions().round_int_to_i64(),
                    &[arg.value, is_signed, precision],
                )
            }
            VarType::F64 => builder.ins().call(
                context.native_functions().round_float_to_i64(),
                &[arg.value, precision],
            ),
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Round,
                    return_type: self.arg.return_type,
                });
            }
        };
        let value = builder.inst_results(call)[0];
        let native_is_present = builder.inst_results(call)[1];
        let native_is_present = builder
            .ins()
            .icmp_imm_u(IntCC::NotEqual, native_is_present, 0);
        let is_present = builder.ins().band(arg.is_present, native_is_present);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

pub(super) fn register_jit_symbols(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(ROUND_FLOAT_SYMBOL, round_float as *const u8);
    jit_builder.symbol(ROUND_INT_TO_I64_SYMBOL, round_int_to_i64 as *const u8);
    jit_builder.symbol(ROUND_FLOAT_TO_I64_SYMBOL, round_float_to_i64 as *const u8);
}

pub(super) struct NativeRoundFunctions {
    pub(super) round_float: FuncRef,
    pub(super) round_int_to_i64: FuncRef,
    pub(super) round_float_to_i64: FuncRef,
}

pub(super) fn declare_native_functions(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
) -> Result<NativeRoundFunctions, CompileError> {
    let mut round_float_signature = module.make_signature();
    round_float_signature
        .params
        .extend([AbiParam::new(types::F64), AbiParam::new(types::I64)]);
    round_float_signature
        .returns
        .push(AbiParam::new(types::F64));
    let round_float_id =
        module.declare_function(ROUND_FLOAT_SYMBOL, Linkage::Import, &round_float_signature)?;

    let mut round_int_signature = module.make_signature();
    round_int_signature.params.extend([
        AbiParam::new(types::I64),
        AbiParam::new(types::I64),
        AbiParam::new(types::I64),
    ]);
    round_int_signature
        .returns
        .extend([AbiParam::new(types::I64), AbiParam::new(types::I64)]);
    let round_int_id = module.declare_function(
        ROUND_INT_TO_I64_SYMBOL,
        Linkage::Import,
        &round_int_signature,
    )?;

    let mut round_float_to_i64_signature = module.make_signature();
    round_float_to_i64_signature
        .params
        .extend([AbiParam::new(types::F64), AbiParam::new(types::I64)]);
    round_float_to_i64_signature
        .returns
        .extend([AbiParam::new(types::I64), AbiParam::new(types::I64)]);
    let round_float_to_i64_id = module.declare_function(
        ROUND_FLOAT_TO_I64_SYMBOL,
        Linkage::Import,
        &round_float_to_i64_signature,
    )?;

    Ok(NativeRoundFunctions {
        round_float: module.declare_func_in_func(round_float_id, function),
        round_int_to_i64: module.declare_func_in_func(round_int_id, function),
        round_float_to_i64: module.declare_func_in_func(round_float_to_i64_id, function),
    })
}

#[repr(C)]
struct RawI64 {
    value: i64,
    is_present: usize,
}

impl RawI64 {
    fn none() -> Self {
        Self {
            value: 0,
            is_present: 0,
        }
    }

    fn some(value: i64) -> Self {
        Self {
            value,
            is_present: 1,
        }
    }
}

extern "C" fn round_float(value: f64, precision: i64) -> f64 {
    round_f64_with_precision(value, precision)
}

extern "C" fn round_int_to_i64(value: u64, is_signed: usize, precision: i64) -> RawI64 {
    let value = if is_signed != 0 {
        value as i64
    } else {
        let Ok(value) = i64::try_from(value) else {
            return RawI64::none();
        };
        value
    };
    match round_i64_with_precision(value, precision) {
        Some(value) => RawI64::some(value),
        None => RawI64::none(),
    }
}

extern "C" fn round_float_to_i64(value: f64, precision: i64) -> RawI64 {
    let rounded = round_f64_with_precision(value, precision);
    if !rounded.is_finite() || rounded < i64::MIN as f64 || rounded >= -(i64::MIN as f64) {
        return RawI64::none();
    }
    RawI64::some(rounded as i64)
}

fn round_i64_with_precision(value: i64, precision: i64) -> Option<i64> {
    if precision >= 0 {
        return Some(value);
    }
    let decimal_places = precision.unsigned_abs();
    if decimal_places > 19 {
        return Some(0);
    }
    let factor = 10i128.pow(decimal_places as u32);
    let value = i128::from(value);
    let quotient = value / factor;
    let remainder = value % factor;
    let adjustment = if remainder.abs() * 2 >= factor {
        value.signum()
    } else {
        0
    };
    i64::try_from((quotient + adjustment) * factor).ok()
}

fn round_f64_with_precision(value: f64, precision: i64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if precision == 0 {
        return value.round();
    }
    if precision > 0 {
        let Ok(decimal_places) = u32::try_from(precision) else {
            return value;
        };
        if decimal_places > 308 {
            return value;
        }
        let factor = 10f64.powi(decimal_places as i32);
        let scaled = value * factor;
        if !scaled.is_finite() {
            return value;
        }
        return scaled.round() / factor;
    }

    let decimal_places = precision.unsigned_abs();
    if decimal_places > 308 {
        return 0.0f64.copysign(value);
    }
    let factor = 10f64.powi(decimal_places as i32);
    (value / factor).round() * factor
}

impl From<RoundFnCall> for FnCallEnum {
    fn from(call: RoundFnCall) -> Self {
        FnCallEnum::Round(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval_i64(expression: &str) -> Option<i64> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: The expression has no inputs and returns a nullable i64 value.
        unsafe { compiled.call(&[]).as_i64() }
    }

    fn eval_f64(expression: &str) -> Option<f64> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: The expression has no inputs and returns a nullable f64 value.
        unsafe { compiled.call(&[]).as_f64() }
    }

    #[test]
    fn test_signature_precision_and_output_type() {
        let expression = deserialize("(ROUND value)").unwrap();
        let inferred = infer_types(&expression).unwrap();
        assert_eq!(inferred.get("value"), Some(&InferredTypeSet::NUMERICAL));

        for expression in ["(ROUND)", "(ROUND 1i64 2i64 3i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Round,
                    expected: 2,
                    ..
                })
            ));
        }

        for (expression, expected) in [
            ("(ROUND 1.2f64)", VarType::I64),
            ("(ROUND 1.2f64 0i64)", VarType::I64),
            ("(ROUND 1.2f64 -1i64)", VarType::I64),
            ("(ROUND 1.2f64 1i64)", VarType::F64),
        ] {
            let expression = deserialize(expression).unwrap();
            assert_eq!(
                compile(&expression, &HashMap::new()).unwrap().result_type(),
                expected
            );
        }
    }

    #[test]
    fn test_units_and_negative_precision_round_away_from_zero() {
        assert_eq!(eval_i64("(ROUND 1.4f64)"), Some(1));
        assert_eq!(eval_i64("(ROUND 1.5f64)"), Some(2));
        assert_eq!(eval_i64("(ROUND -1.4f64)"), Some(-1));
        assert_eq!(eval_i64("(ROUND -1.5f64)"), Some(-2));
        assert_eq!(eval_i64("(ROUND 149i64 -2i64)"), Some(100));
        assert_eq!(eval_i64("(ROUND 150i64 -2i64)"), Some(200));
        assert_eq!(eval_i64("(ROUND -150i64 -2i64)"), Some(-200));
        assert_eq!(
            eval_i64("(ROUND -9223372036854775808i64 0i64)"),
            Some(i64::MIN)
        );
    }

    #[test]
    fn test_positive_precision_returns_float() {
        assert_eq!(eval_f64("(ROUND 1.234f64 2i64)"), Some(1.23));
        assert_eq!(eval_f64("(ROUND 1.235f64 2i64)"), Some(1.24));
        assert_eq!(eval_f64("(ROUND -1.235f64 2i64)"), Some(-1.24));
        assert_eq!(eval_f64("(ROUND 123i64 2i64)"), Some(123.0));
        assert_eq!(eval_f64("(ROUND 1e308f64 2i64)"), Some(1e308));
        assert_eq!(eval_f64("(ROUND 1.25f64 400i64)"), Some(1.25));
    }

    #[test]
    fn test_null_nonfinite_and_range_edges() {
        assert_eq!(eval_i64("(ROUND none)"), None);
        assert_eq!(eval_i64("(ROUND 1.2f64 none)"), None);
        assert_eq!(eval_i64("(ROUND nanf64)"), None);
        assert_eq!(eval_i64("(ROUND inff64)"), None);
        assert_eq!(eval_i64("(ROUND 9223372036854775808f64)"), None);
        assert_eq!(eval_i64("(ROUND 18446744073709551615u64)"), None);
        assert_eq!(eval_i64("(ROUND 123i64 -400i64)"), Some(0));

        assert!(eval_f64("(ROUND nanf64 2i64)").unwrap().is_nan());
        assert_eq!(eval_f64("(ROUND inff64 2i64)"), Some(f64::INFINITY));
    }

    #[test]
    fn test_runtime_null_and_integer_rounding() {
        let expression = deserialize("(ROUND value -1i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::I64)])).unwrap();

        // SAFETY: The compiled expression expects one nullable i64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some(155i64)]).as_i64() },
            Some(160)
        );
        // SAFETY: The compiled expression expects one nullable i64 argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_i64() },
            None
        );
    }
}
