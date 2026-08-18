use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type};
use cranelift_jit::{JITBuilder, JITModule};

use super::{eq, lower, regexp_extract};
use crate::compile::CompileError;

/// References to native functions imported into the current Cranelift function.
pub(crate) struct NativeFunctions {
    string_eq: FuncRef,
    string_lowercase: FuncRef,
    regexp_extract: FuncRef,
}

impl NativeFunctions {
    pub(crate) fn string_eq(&self) -> FuncRef {
        self.string_eq
    }

    pub(crate) fn string_lowercase(&self) -> FuncRef {
        self.string_lowercase
    }

    pub(crate) fn regexp_extract(&self) -> FuncRef {
        self.regexp_extract
    }
}

/// Registers the process symbols that native calls may reference from generated code.
pub(crate) fn register_jit_symbols(jit_builder: &mut JITBuilder) {
    eq::register_jit_symbol(jit_builder);
    lower::register_jit_symbol(jit_builder);
    regexp_extract::register_jit_symbol(jit_builder);
}

/// Declares every native function imported by the expression being compiled.
pub(crate) fn declare_native_functions(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<NativeFunctions, CompileError> {
    Ok(NativeFunctions {
        string_eq: eq::declare_native_function(module, function, pointer_type)?,
        string_lowercase: lower::declare_native_function(module, function, pointer_type)?,
        regexp_extract: regexp_extract::declare_native_function(module, function, pointer_type)?,
    })
}
