use cranelift_jit::JITModule;

use super::{TypedExpr, TypedVariable};
use crate::types::{VarType, VariableValue};

#[cfg(not(any(
    all(target_arch = "x86_64", not(target_os = "windows")),
    target_arch = "aarch64"
)))]
compile_error!(
    "the direct VariableValue JIT return ABI is only implemented for x86-64 System V and AArch64"
);

// On the supported targets, VariableValue's two eightbytes are returned in two
// integer registers by the platform C ABI. The lifetime is selected by
// CompiledFn::call so that it is bounded by both possible sources of strings.
// This is a Rust-to-JIT boundary whose VariableValue layout is asserted in
// types.rs, not an interface intended for C callers.
#[allow(improper_ctypes_definitions)]
pub(crate) type JitEntry =
    for<'a> unsafe extern "C" fn(*const VariableValue<'a>) -> VariableValue<'a>;

/// An expression compiled to native machine code.
///
/// This object owns the JIT module containing its executable memory and every
/// resource referenced by the generated code.
pub struct CompiledFn {
    pub(crate) entry: JitEntry,
    pub(crate) _module: JITModule,
    /// Input slots in the exact order expected by [`CompiledFn::call`].
    pub inputs: Vec<TypedVariable>,
    // This AST owns the Arc-backed literals and regexes embedded in generated code.
    pub(crate) _typed_expr: Box<TypedExpr>,
}

impl CompiledFn {
    /// Returns the concrete result type selected during compilation.
    pub fn result_type(&self) -> VarType {
        self._typed_expr.return_type
    }

    /// Evaluate the compiled expression.
    ///
    /// # Safety
    ///
    /// `args` must follow [`CompiledFn::inputs`] exactly: every present slot must
    /// contain the union member corresponding to that variable's type. Absent
    /// slots must use [`VariableValue::none`], and any borrowed strings must
    /// remain alive for the duration of this call.
    ///
    /// The result cannot outlive the compiled function nor the passed
    /// arguments's lifetime.
    pub unsafe fn call<'args, 'compiled, 'output>(
        &'compiled self,
        args: &[VariableValue<'args>],
    ) -> VariableValue<'output>
    where
        'args: 'output,
        'compiled: 'output,
    {
        debug_assert_eq!(args.len(), self.inputs.len());
        let args: &[VariableValue<'output>] = args;
        // SAFETY: Guaranteed by the caller. Both the input and compiled-function
        // lifetimes outlive the lifetime selected for the returned value.
        unsafe { (self.entry)(args.as_ptr()) }
    }
}
