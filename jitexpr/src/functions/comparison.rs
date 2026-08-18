//! Shared lowering and native helpers for ordered comparisons.

use std::cmp::Ordering;
use std::collections::HashMap;

use cranelift::codegen::ir::{
    FuncRef, Function as CraneliftFunction, Type, Value as CraneliftValue, types,
};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, FloatCC, InstBuilder, IntCC};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr};
use crate::types::VarType;

const STRING_COMPARE_SYMBOL: &str = "jitexpr_string_compare";
const F64_I64_COMPARE_SYMBOL: &str = "jitexpr_f64_i64_compare";
const F64_U64_COMPARE_SYMBOL: &str = "jitexpr_f64_u64_compare";
const UNORDERED: i8 = i8::MIN;

const ORDERED_TYPES: InferredTypeSet = InferredTypeSet {
    string: true,
    i64: true,
    u64: true,
    f64: true,
    boolean: false,
};

#[derive(Clone, Copy)]
#[allow(dead_code)] // Variants become live as the four comparison functions are added separately.
pub(super) enum OrderedComparison {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

pub(super) fn infer_types<'a>(
    function: Function,
    args: &'a [UntypedExpr],
    target_type: InferredTypeSet,
    inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
) -> Result<InferredTypeSet, TypeError> {
    if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
        return Err(TypeError::WrongFunctionReturnType {
            function,
            expected: target_type,
            got: InferredTypeSet::BOOLEAN,
        });
    }
    if args.len() != 2 {
        return Err(TypeError::InvalidNumberOfArguments {
            function,
            expected: 2,
            got: args.len(),
        });
    }

    for arg in args {
        crate::ast::infer_types_aux(arg, ORDERED_TYPES, inferred_types)?;
    }
    Ok(InferredTypeSet::BOOLEAN)
}

pub(super) fn apply_types(
    args: &[UntypedExpr],
    context: &mut CompileFnBuilder<'_, '_>,
) -> Result<Box<[TypedExpr]>, CompileError> {
    args.iter()
        .map(|arg| match arg {
            UntypedExpr::Literal(literal) => {
                context.apply_types(arg, InferredTypeSet::singleton(literal.r#type()))
            }
            _ => context.apply_types(arg, ORDERED_TYPES),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Vec::into_boxed_slice)
}

pub(super) fn lower(
    args: &[TypedExpr],
    comparison: OrderedComparison,
    context: &mut LoweringContext<'_>,
    builder: &mut FunctionBuilder<'_>,
) -> Result<LoweredValue, CompileError> {
    let lhs_type = args[0].return_type;
    let rhs_type = args[1].return_type;
    let lhs = context.compile_expr(&args[0], builder)?;
    let rhs = context.compile_expr(&args[1], builder)?;
    let is_present = builder.ins().band(lhs.is_present, rhs.is_present);
    let string_len = builder.ins().iconst(types::I64, 0);

    let compared = if lhs_type == VarType::None || rhs_type == VarType::None {
        builder.ins().iconst(types::I8, 0)
    } else if lhs_type == VarType::Str && rhs_type == VarType::Str {
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let lhs_ptr = builder.ins().select(lhs.is_present, lhs.value, null);
        let rhs_ptr = builder.ins().select(rhs.is_present, rhs.value, null);
        let call = builder.ins().call(
            context.native_functions().string_compare(),
            &[lhs_ptr, lhs.string_len, rhs_ptr, rhs.string_len],
        );
        compare_ordering(builder.inst_results(call)[0], comparison, builder)
    } else if is_numerical(lhs_type) && is_numerical(rhs_type) {
        lower_numeric(lhs, lhs_type, rhs, rhs_type, comparison, context, builder)
    } else {
        builder.ins().iconst(types::I8, 0)
    };
    let value = builder.ins().band(compared, is_present);
    Ok(LoweredValue {
        value,
        is_present,
        string_len,
    })
}

fn lower_numeric(
    lhs: LoweredValue,
    lhs_type: VarType,
    rhs: LoweredValue,
    rhs_type: VarType,
    comparison: OrderedComparison,
    context: &mut LoweringContext<'_>,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    match (lhs_type, rhs_type) {
        (VarType::I64, VarType::I64) => {
            builder
                .ins()
                .icmp(comparison.signed_int_cc(), lhs.value, rhs.value)
        }
        (VarType::U64, VarType::U64) => {
            builder
                .ins()
                .icmp(comparison.unsigned_int_cc(), lhs.value, rhs.value)
        }
        (VarType::F64, VarType::F64) => {
            builder
                .ins()
                .fcmp(comparison.float_cc(), lhs.value, rhs.value)
        }
        (VarType::I64, VarType::U64) => {
            let ordering = compare_signed_unsigned(lhs.value, rhs.value, builder);
            compare_ordering(ordering, comparison, builder)
        }
        (VarType::U64, VarType::I64) => {
            let ordering = compare_signed_unsigned(rhs.value, lhs.value, builder);
            let reversed = builder.ins().ineg(ordering);
            compare_ordering(reversed, comparison, builder)
        }
        (VarType::F64, VarType::I64) => {
            let call = builder.ins().call(
                context.native_functions().f64_i64_compare(),
                &[lhs.value, rhs.value],
            );
            compare_ordering(builder.inst_results(call)[0], comparison, builder)
        }
        (VarType::I64, VarType::F64) => {
            let call = builder.ins().call(
                context.native_functions().f64_i64_compare(),
                &[rhs.value, lhs.value],
            );
            let ordering = builder.inst_results(call)[0];
            let reversed = builder.ins().ineg(ordering);
            compare_ordering(reversed, comparison, builder)
        }
        (VarType::F64, VarType::U64) => {
            let call = builder.ins().call(
                context.native_functions().f64_u64_compare(),
                &[lhs.value, rhs.value],
            );
            compare_ordering(builder.inst_results(call)[0], comparison, builder)
        }
        (VarType::U64, VarType::F64) => {
            let call = builder.ins().call(
                context.native_functions().f64_u64_compare(),
                &[rhs.value, lhs.value],
            );
            let ordering = builder.inst_results(call)[0];
            let reversed = builder.ins().ineg(ordering);
            compare_ordering(reversed, comparison, builder)
        }
        _ => unreachable!("ordered numeric comparison received non-numeric types"),
    }
}

fn compare_signed_unsigned(
    signed: CraneliftValue,
    unsigned: CraneliftValue,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    let less = builder
        .ins()
        .icmp(IntCC::UnsignedLessThan, signed, unsigned);
    let greater = builder
        .ins()
        .icmp(IntCC::UnsignedGreaterThan, signed, unsigned);
    let minus_one = builder.ins().iconst(types::I8, -1);
    let zero = builder.ins().iconst(types::I8, 0);
    let one = builder.ins().iconst(types::I8, 1);
    let greater_or_equal = builder.ins().select(greater, one, zero);
    let nonnegative_ordering = builder.ins().select(less, minus_one, greater_or_equal);
    let is_negative = builder.ins().icmp_imm_s(IntCC::SignedLessThan, signed, 0);
    builder
        .ins()
        .select(is_negative, minus_one, nonnegative_ordering)
}

fn compare_ordering(
    ordering: CraneliftValue,
    comparison: OrderedComparison,
    builder: &mut FunctionBuilder<'_>,
) -> CraneliftValue {
    let ordered = builder
        .ins()
        .icmp_imm_s(IntCC::NotEqual, ordering, i64::from(UNORDERED));
    let relation = builder
        .ins()
        .icmp_imm_s(comparison.ordering_cc(), ordering, 0);
    builder.ins().band(ordered, relation)
}

fn is_numerical(var_type: VarType) -> bool {
    matches!(var_type, VarType::I64 | VarType::U64 | VarType::F64)
}

impl OrderedComparison {
    fn signed_int_cc(self) -> IntCC {
        match self {
            Self::GreaterThan => IntCC::SignedGreaterThan,
            Self::LessThan => IntCC::SignedLessThan,
            Self::GreaterThanOrEqual => IntCC::SignedGreaterThanOrEqual,
            Self::LessThanOrEqual => IntCC::SignedLessThanOrEqual,
        }
    }

    fn unsigned_int_cc(self) -> IntCC {
        match self {
            Self::GreaterThan => IntCC::UnsignedGreaterThan,
            Self::LessThan => IntCC::UnsignedLessThan,
            Self::GreaterThanOrEqual => IntCC::UnsignedGreaterThanOrEqual,
            Self::LessThanOrEqual => IntCC::UnsignedLessThanOrEqual,
        }
    }

    fn float_cc(self) -> FloatCC {
        match self {
            Self::GreaterThan => FloatCC::GreaterThan,
            Self::LessThan => FloatCC::LessThan,
            Self::GreaterThanOrEqual => FloatCC::GreaterThanOrEqual,
            Self::LessThanOrEqual => FloatCC::LessThanOrEqual,
        }
    }

    fn ordering_cc(self) -> IntCC {
        match self {
            Self::GreaterThan => IntCC::SignedGreaterThan,
            Self::LessThan => IntCC::SignedLessThan,
            Self::GreaterThanOrEqual => IntCC::SignedGreaterThanOrEqual,
            Self::LessThanOrEqual => IntCC::SignedLessThanOrEqual,
        }
    }
}

pub(super) fn register_jit_symbols(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(STRING_COMPARE_SYMBOL, string_compare as *const u8);
    jit_builder.symbol(F64_I64_COMPARE_SYMBOL, f64_i64_compare as *const u8);
    jit_builder.symbol(F64_U64_COMPARE_SYMBOL, f64_u64_compare as *const u8);
}

pub(super) struct NativeComparisonFunctions {
    pub(super) string_compare: FuncRef,
    pub(super) f64_i64_compare: FuncRef,
    pub(super) f64_u64_compare: FuncRef,
}

pub(super) fn declare_native_functions(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<NativeComparisonFunctions, CompileError> {
    let mut string_signature = module.make_signature();
    string_signature.params.extend([
        AbiParam::new(pointer_type),
        AbiParam::new(types::I64),
        AbiParam::new(pointer_type),
        AbiParam::new(types::I64),
    ]);
    string_signature.returns.push(AbiParam::new(types::I8));
    let string_function =
        module.declare_function(STRING_COMPARE_SYMBOL, Linkage::Import, &string_signature)?;

    let mut numeric_signature = module.make_signature();
    numeric_signature
        .params
        .extend([AbiParam::new(types::F64), AbiParam::new(types::I64)]);
    numeric_signature.returns.push(AbiParam::new(types::I8));
    let f64_i64_function =
        module.declare_function(F64_I64_COMPARE_SYMBOL, Linkage::Import, &numeric_signature)?;
    let f64_u64_function =
        module.declare_function(F64_U64_COMPARE_SYMBOL, Linkage::Import, &numeric_signature)?;

    Ok(NativeComparisonFunctions {
        string_compare: module.declare_func_in_func(string_function, function),
        f64_i64_compare: module.declare_func_in_func(f64_i64_function, function),
        f64_u64_compare: module.declare_func_in_func(f64_u64_function, function),
    })
}

unsafe extern "C" fn string_compare(
    lhs_ptr: *const u8,
    lhs_len: usize,
    rhs_ptr: *const u8,
    rhs_len: usize,
) -> i8 {
    if lhs_ptr.is_null() || rhs_ptr.is_null() {
        return 0;
    }
    // SAFETY: Generated code passes live UTF-8 pointers and their exact byte lengths for present
    // string values. Null pointers were rejected above.
    let lhs =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(lhs_ptr, lhs_len)) };
    // SAFETY: Same contract as `lhs`.
    let rhs =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(rhs_ptr, rhs_len)) };
    ordering_code(lhs.cmp(rhs))
}

extern "C" fn f64_i64_compare(lhs: f64, rhs: i64) -> i8 {
    if lhs.is_nan() {
        return UNORDERED;
    }
    if lhs < i64::MIN as f64 {
        return -1;
    }
    if lhs >= -(i64::MIN as f64) {
        return 1;
    }
    let truncated = lhs as i64;
    match truncated.cmp(&rhs) {
        Ordering::Equal => ordering_code(lhs.partial_cmp(&(rhs as f64)).unwrap()),
        ordering => ordering_code(ordering),
    }
}

extern "C" fn f64_u64_compare(lhs: f64, rhs: u64) -> i8 {
    if lhs.is_nan() {
        return UNORDERED;
    }
    if lhs < 0.0 {
        return -1;
    }
    if lhs >= 2f64.powi(64) {
        return 1;
    }
    let truncated = lhs as u64;
    match truncated.cmp(&rhs) {
        Ordering::Equal => ordering_code(lhs.partial_cmp(&(rhs as f64)).unwrap()),
        ordering => ordering_code(ordering),
    }
}

fn ordering_code(ordering: Ordering) -> i8 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}
