// EQ compares two values of any type.
//
// Values of the same type compare directly. Numerical values also compare across
// i64, u64, and f64. Values of unrelated types are not equal.
//
// In other words:
// 1u64 == 1i64 ==> true
// 1f64 == 1u64 ==> true
// 1.2f64 == 1u64 ==> false
// 1f64 == "1" ==> false
// "1" == "1" ==> true

use std::collections::HashMap;

use cranelift::codegen::ir::{
    FuncRef, Function as CraneliftFunction, Type, Value as CraneliftValue, types,
};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, FloatCC, InstBuilder, IntCC};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const STRING_EQ_SYMBOL: &str = "jitexpr_string_eq";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EqFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for EqFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Eq,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Eq,
                expected: 2,
                got: args.len(),
            });
        }
        // TODO actually we probably want to be stricter here, so that we pick the right column in
        // the end. thing my_col == 1i64.
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
        assert_eq!(args.len(), 2, "Expected 2 args for EQ");
        debug_assert!(target_type_set.contains(VarType::Bool));

        // EQ must retain each literal's declared type. In contrast with ADD, it
        // does not need to choose one common arithmetic type for its operands.
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
            ast: TypedExprAst::from_call(EqFnCall {
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
        debug_assert_eq!(return_type, VarType::Bool);
        let lhs_type = self.args[0].return_type;
        let rhs_type = self.args[1].return_type;
        let lhs = context.compile_expr(&self.args[0], builder)?;
        let rhs = context.compile_expr(&self.args[1], builder)?;
        let is_present = builder.ins().iconst(types::I8, 1);
        let string_len = builder.ins().iconst(types::I64, 0);

        if lhs_type == VarType::None || rhs_type == VarType::None {
            let value = builder
                .ins()
                .icmp(IntCC::Equal, lhs.is_present, rhs.is_present);
            return Ok(LoweredValue {
                value,
                is_present,
                string_len,
            });
        }

        if lhs_type == VarType::Str && rhs_type == VarType::Str {
            let null = builder.ins().iconst(context.pointer_type(), 0);
            let lhs_ptr = builder.ins().select(lhs.is_present, lhs.value, null);
            let rhs_ptr = builder.ins().select(rhs.is_present, rhs.value, null);
            let call = builder.ins().call(
                context.native_functions().string_eq(),
                &[lhs_ptr, lhs.string_len, rhs_ptr, rhs.string_len],
            );
            return Ok(LoweredValue {
                value: builder.inst_results(call)[0],
                is_present,
                string_len,
            });
        }

        if !types_are_comparable(lhs_type, rhs_type) {
            let value = both_absent(lhs.is_present, rhs.is_present, builder);
            return Ok(LoweredValue {
                value,
                is_present,
                string_len,
            });
        }

        let values_equal = match (lhs_type, rhs_type) {
            (VarType::Bool, VarType::Bool)
            | (VarType::I64, VarType::I64)
            | (VarType::U64, VarType::U64) => {
                builder.ins().icmp(IntCC::Equal, lhs.value, rhs.value)
            }
            (VarType::F64, VarType::F64) => {
                builder.ins().fcmp(FloatCC::Equal, lhs.value, rhs.value)
            }
            (VarType::I64, VarType::U64) => emit_signed_unsigned_eq(lhs.value, rhs.value, builder),
            (VarType::U64, VarType::I64) => emit_signed_unsigned_eq(rhs.value, lhs.value, builder),
            (VarType::F64, VarType::I64) => {
                emit_float_integer_eq(lhs.value, rhs.value, VarType::I64, builder)
            }
            (VarType::I64, VarType::F64) => {
                emit_float_integer_eq(rhs.value, lhs.value, VarType::I64, builder)
            }
            (VarType::F64, VarType::U64) => {
                emit_float_integer_eq(lhs.value, rhs.value, VarType::U64, builder)
            }
            (VarType::U64, VarType::F64) => {
                emit_float_integer_eq(rhs.value, lhs.value, VarType::U64, builder)
            }
            _ => unreachable!("the operand types were checked above"),
        };
        let both_present = builder.ins().band(lhs.is_present, rhs.is_present);
        let present_and_equal = builder.ins().band(both_present, values_equal);
        let both_absent = both_absent(lhs.is_present, rhs.is_present, builder);
        let value = builder.ins().bor(both_absent, present_and_equal);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

fn both_absent(
    lhs_is_present: CraneliftValue,
    rhs_is_present: CraneliftValue,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    let lhs_absent = builder.ins().bxor_imm_u(lhs_is_present, 1);
    let rhs_absent = builder.ins().bxor_imm_u(rhs_is_present, 1);
    builder.ins().band(lhs_absent, rhs_absent)
}

fn types_are_comparable(lhs: VarType, rhs: VarType) -> bool {
    lhs == rhs || (is_numerical(lhs) && is_numerical(rhs))
}

fn is_numerical(var_type: VarType) -> bool {
    matches!(var_type, VarType::I64 | VarType::U64 | VarType::F64)
}

fn emit_signed_unsigned_eq(
    signed: CraneliftValue,
    unsigned: CraneliftValue,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    let nonnegative = builder
        .ins()
        .icmp_imm_s(IntCC::SignedGreaterThanOrEqual, signed, 0);
    let same_bits = builder.ins().icmp(IntCC::Equal, signed, unsigned);
    builder.ins().band(nonnegative, same_bits)
}

fn emit_float_integer_eq(
    float: CraneliftValue,
    integer: CraneliftValue,
    integer_type: VarType,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    let (lower_bound, upper_bound) = match integer_type {
        VarType::I64 => (i64::MIN as f64, -(i64::MIN as f64)),
        VarType::U64 => (0.0, (u64::MAX as f64)),
        _ => unreachable!("EQ only compares f64 to i64 or u64 here"),
    };
    let lower_bound = builder.ins().f64const(lower_bound);
    let upper_bound = builder.ins().f64const(upper_bound);
    let above_lower = builder
        .ins()
        .fcmp(FloatCC::GreaterThanOrEqual, float, lower_bound);
    let below_upper = builder.ins().fcmp(FloatCC::LessThan, float, upper_bound);
    let in_range = builder.ins().band(above_lower, below_upper);

    let converted = match integer_type {
        VarType::I64 => builder.ins().fcvt_to_sint_sat(types::I64, float),
        VarType::U64 => builder.ins().fcvt_to_uint_sat(types::I64, float),
        _ => unreachable!("EQ only compares f64 to i64 or u64 here"),
    };
    let same_integer = builder.ins().icmp(IntCC::Equal, converted, integer);
    let round_trip = match integer_type {
        VarType::I64 => builder.ins().fcvt_from_sint(types::F64, converted),
        VarType::U64 => builder.ins().fcvt_from_uint(types::F64, converted),
        _ => unreachable!("EQ only compares f64 to i64 or u64 here"),
    };
    let is_integral = builder.ins().fcmp(FloatCC::Equal, float, round_trip);
    let equal = builder.ins().band(in_range, same_integer);
    builder.ins().band(equal, is_integral)
}

pub(super) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(STRING_EQ_SYMBOL, string_eq as *const u8);
}

pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature.params.extend([
        AbiParam::new(pointer_type),
        AbiParam::new(types::I64),
        AbiParam::new(pointer_type),
        AbiParam::new(types::I64),
    ]);
    signature.returns.push(AbiParam::new(types::I8));
    let function_id = module.declare_function(STRING_EQ_SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

unsafe extern "C" fn string_eq(
    lhs_ptr: *const u8,
    lhs_len: usize,
    rhs_ptr: *const u8,
    rhs_len: usize,
) -> u8 {
    let lhs = if lhs_ptr.is_null() {
        None
    } else {
        // SAFETY: Generated code passes a live UTF-8 string pointer and its
        // exact byte length whenever the pointer is non-null.
        Some(unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(lhs_ptr, lhs_len)) })
    };
    let rhs = if rhs_ptr.is_null() {
        None
    } else {
        // SAFETY: Same contract as `lhs` above.
        Some(unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(rhs_ptr, rhs_len)) })
    };
    match (lhs, rhs) {
        (None, None) => 1,
        (Some(lhs), Some(rhs)) => u8::from(lhs == rhs),
        _ => 0,
    }
}

impl From<EqFnCall> for FnCallEnum {
    fn from(call: EqFnCall) -> Self {
        FnCallEnum::Eq(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{self, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> bool {
        let expression = ast::deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };

        unsafe { output.as_bool() }.unwrap()
    }

    #[test]
    fn test_infer_types_accepts_any_operand_types() {
        let expression = ast::deserialize(r#"(EQ value "hello")"#).unwrap();

        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::ALL));
    }

    #[test]
    fn test_infer_types_requires_two_arguments() {
        let expression = Function::Eq.call_untyped_expr(vec![UntypedExpr::literal(1i64)]);

        let error = infer_types(&expression).unwrap_err();

        assert!(matches!(
            error,
            TypeError::InvalidNumberOfArguments {
                function: Function::Eq,
                expected: 2,
                got: 1,
            }
        ));
    }

    #[test]
    fn test_compile_numeric_examples() {
        assert!(eval("(EQ 1u64 1i64)"));
        assert!(eval("(EQ 1f64 1i64)"));
        assert!(!eval("(EQ 1.2f64 1i64)"));
    }

    #[test]
    fn test_compile_different_types_are_not_equal() {
        assert!(!eval(r#"(EQ 1i64 "1")"#));
        assert!(!eval("(EQ true 1u64)"));
    }

    #[test]
    fn test_compile_signed_unsigned_comparison() {
        let expression = ast::deserialize("(EQ signed unsigned)").unwrap();
        let variable_types = HashMap::from([("signed", VarType::I64), ("unsigned", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        for (signed, unsigned, expected) in [(7i64, 7u64, true), (-1, u64::MAX, false)] {
            let input = [VariableValue::some(signed), VariableValue::some(unsigned)];
            let output = unsafe { compiled.call(&input) };
            assert_eq!(unsafe { output.as_bool() }, Some(expected));
        }
    }

    #[test]
    fn test_compile_float_integer_comparison_is_exact() {
        let expression = ast::deserialize("(EQ float integer)").unwrap();
        let variable_types = HashMap::from([("float", VarType::F64), ("integer", VarType::I64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let cases = [
            (1.0, 1, true),
            (1.2, 1, false),
            ((1u64 << 53) as f64, (1i64 << 53) + 1, false),
            (i64::MIN as f64, i64::MIN, true),
            (2f64.powi(63), i64::MAX, false),
        ];

        for (float, integer, expected) in cases {
            let input = [VariableValue::some(float), VariableValue::some(integer)];
            let output = unsafe { compiled.call(&input) };
            assert_eq!(unsafe { output.as_bool() }, Some(expected));
        }
    }

    #[test]
    fn test_compile_float_unsigned_comparison_is_exact() {
        let expression = ast::deserialize("(EQ float integer)").unwrap();
        let variable_types = HashMap::from([("float", VarType::F64), ("integer", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let cases = [
            (1.0, 1, true),
            (1.2, 1, false),
            (2f64.powi(63), 1u64 << 63, true),
            (u64::MAX as f64, u64::MAX, false),
            (f64::NAN, 0, false),
        ];

        for (float, integer, expected) in cases {
            let input = [VariableValue::some(float), VariableValue::some(integer)];
            let output = unsafe { compiled.call(&input) };
            assert_eq!(unsafe { output.as_bool() }, Some(expected));
        }
    }

    #[test]
    fn test_compile_string_equality_compares_contents() {
        let expression = ast::deserialize("(EQ left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let left_value = String::from("same contents");
        let right_value = String::from("same contents");
        let input = [
            VariableValue::some(left_value.as_str()),
            VariableValue::some(right_value.as_str()),
        ];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_bool() }, Some(true));

        let different_value = String::from("different contents");
        let input = [
            VariableValue::some(left_value.as_str()),
            VariableValue::some(different_value.as_str()),
        ];
        let output = unsafe { compiled.call(&input) };
        assert_eq!(unsafe { output.as_bool() }, Some(false));

        let none_input = [VariableValue::none(), VariableValue::none()];
        let output = unsafe { compiled.call(&none_input) };
        assert_eq!(unsafe { output.as_bool() }, Some(true));
    }

    #[test]
    fn test_compile_none_equality() {
        assert!(eval("(EQ none none)"));
        assert!(!eval("(EQ none false)"));
    }

    #[test]
    fn test_compile_runtime_none_equality() {
        let expression = ast::deserialize("(EQ left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::U64), ("right", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let output = unsafe { compiled.call(&[VariableValue::none(), VariableValue::none()]) };
        assert_eq!(unsafe { output.as_bool() }, Some(true));

        let output = unsafe { compiled.call(&[VariableValue::none(), VariableValue::some(0u64)]) };
        assert_eq!(unsafe { output.as_bool() }, Some(false));
    }

    #[test]
    fn test_compile_none_literal_equals_absent_variable() {
        let expression = ast::deserialize("(EQ value none)").unwrap();
        let variable_types = HashMap::from([("value", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let output = unsafe { compiled.call(&[VariableValue::none()]) };

        assert_eq!(unsafe { output.as_bool() }, Some(true));
    }

    #[test]
    fn test_call_with_types_preserves_literal_types() {
        let typed_expr = crate::typed_expr_from_str("(EQ 1u64 1f64)", &HashMap::new());
        let TypedExprAst::FnCall(FnCallEnum::Eq(call)) = typed_expr.ast else {
            panic!("expected an EQ call");
        };

        assert_eq!(call.args[0].return_type, VarType::U64);
        assert_eq!(call.args[1].return_type, VarType::F64);
    }

    #[test]
    fn test_compile_boolean_equality() {
        assert!(eval("(EQ true true)"));
        assert!(!eval("(EQ true false)"));
    }
}
