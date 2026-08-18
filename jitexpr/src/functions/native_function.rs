use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type};
use cranelift_jit::{JITBuilder, JITModule};

use super::{comparison, concat, eq, int_mod, lower, pow, regexp_extract, trim, upper};
use crate::compile::CompileError;

/// References to native functions imported into the current Cranelift function.
pub(crate) struct NativeFunctions {
    string_eq: FuncRef,
    string_lowercase: FuncRef,
    string_uppercase: FuncRef,
    string_trim: FuncRef,
    string_concat: FuncRef,
    float_mod: FuncRef,
    float_pow: FuncRef,
    regexp_extract: FuncRef,
    string_compare: FuncRef,
    f64_i64_compare: FuncRef,
    f64_u64_compare: FuncRef,
}

impl NativeFunctions {
    pub(crate) fn string_eq(&self) -> FuncRef {
        self.string_eq
    }

    pub(crate) fn string_lowercase(&self) -> FuncRef {
        self.string_lowercase
    }

    pub(crate) fn string_uppercase(&self) -> FuncRef {
        self.string_uppercase
    }
    pub(crate) fn string_trim(&self) -> FuncRef {
        self.string_trim
    }

    pub(crate) fn string_concat(&self) -> FuncRef {
        self.string_concat
    }

    pub(crate) fn float_mod(&self) -> FuncRef {
        self.float_mod
    }

    pub(crate) fn float_pow(&self) -> FuncRef {
        self.float_pow
    }

    pub(crate) fn regexp_extract(&self) -> FuncRef {
        self.regexp_extract
    }

    pub(crate) fn string_compare(&self) -> FuncRef {
        self.string_compare
    }

    pub(crate) fn f64_i64_compare(&self) -> FuncRef {
        self.f64_i64_compare
    }

    pub(crate) fn f64_u64_compare(&self) -> FuncRef {
        self.f64_u64_compare
    }
}

/// Registers the process symbols that native calls may reference from generated code.
pub(crate) fn register_jit_symbols(jit_builder: &mut JITBuilder) {
    eq::register_jit_symbol(jit_builder);
    comparison::register_jit_symbols(jit_builder);
    lower::register_jit_symbol(jit_builder);
    upper::register_jit_symbol(jit_builder);
    trim::register_jit_symbol(jit_builder);
    concat::register_jit_symbol(jit_builder);
    int_mod::register_jit_symbol(jit_builder);
    pow::register_jit_symbol(jit_builder);
    regexp_extract::register_jit_symbol(jit_builder);
}

/// Declares every native function imported by the expression being compiled.
pub(crate) fn declare_native_functions(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<NativeFunctions, CompileError> {
    let comparison = comparison::declare_native_functions(module, function, pointer_type)?;
    Ok(NativeFunctions {
        string_eq: eq::declare_native_function(module, function, pointer_type)?,
        string_lowercase: lower::declare_native_function(module, function, pointer_type)?,
        string_uppercase: upper::declare_native_function(module, function, pointer_type)?,
        string_trim: trim::declare_native_function(module, function, pointer_type)?,
        string_concat: concat::declare_native_function(module, function, pointer_type)?,
        float_mod: int_mod::declare_native_function(module, function)?,
        float_pow: pow::declare_native_function(module, function)?,
        regexp_extract: regexp_extract::declare_native_function(module, function, pointer_type)?,
        string_compare: comparison.string_compare,
        f64_i64_compare: comparison.f64_i64_compare,
        f64_u64_compare: comparison.f64_u64_compare,
    })
}
