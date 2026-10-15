//! Lowering of the `Str -> F64` coercion.
//!
//! The string is parsed with `str::parse::<f64>()`, without trimming. The result is null if the
//! string is null, cannot be parsed, or is not finite (`"NaN"`, `"inf"`, ...).

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, FloatCC, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::compile::{CompileError, LoweredValue, LoweringContext};

const SYMBOL: &str = "jitexpr_str_to_f64";

/// Emits the coercion of the lowered string `string` to `f64`.
pub(super) fn lower_str_to_f64(
    string: LoweredValue,
    context: &mut LoweringContext<'_>,
    builder: &mut FunctionBuilder<'_>,
) -> LoweredValue {
    let null = builder.ins().iconst(context.pointer_type(), 0);
    let string_ptr = builder.ins().select(string.is_present, string.value, null);
    let call = builder.ins().call(
        context.native_functions().str_to_f64(),
        &[string_ptr, string.string_len],
    );
    let value = builder.inst_results(call)[0];
    // The native function returns NaN for an absent result.
    let is_nan = builder.ins().fcmp(FloatCC::Unordered, value, value);
    let is_not_nan = builder.ins().bxor_imm_u(is_nan, 1);
    let is_present = builder.ins().band(string.is_present, is_not_nan);
    LoweredValue {
        value,
        is_present,
        string_len: builder.ins().iconst(types::I64, 0),
    }
}

pub(crate) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(SYMBOL, str_to_f64 as *const u8);
}

pub(crate) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(types::I64));
    signature.returns.push(AbiParam::new(types::F64));
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

/// Runtime implementation called by generated code for the `Str -> F64` coercion.
///
/// Returns NaN for a null pointer, a string that cannot be parsed, or a non-finite value. The
/// generated code relies on this: NaN means that the result is absent.
unsafe extern "C" fn str_to_f64(string_ptr: *const u8, string_len: usize) -> f64 {
    if string_ptr.is_null() {
        return f64::NAN;
    }
    // SAFETY: The contract of CompiledFn::call requires a live UTF-8 string pointer and its exact
    // byte length for every present string input. Strings built by the generated code follow the
    // same contract.
    let string = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(string_ptr, string_len))
    };
    match string.parse::<f64>() {
        Ok(value) if value.is_finite() => value,
        _ => f64::NAN,
    }
}
