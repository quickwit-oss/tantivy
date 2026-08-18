use std::cell::UnsafeCell;
use std::mem;
use std::sync::Arc;

use cranelift_jit::JITModule;
use regex::Regex;

use super::{TypedExpr, TypedVariable};
use crate::types::{StringRef, VarType, VariableOpt};

#[cfg(not(any(
    all(target_arch = "x86_64", not(target_os = "windows")),
    target_arch = "aarch64"
)))]
compile_error!(
    "the direct VariableOpt JIT return ABI is only implemented for x86-64 System V and AArch64"
);

// On the supported targets, VariableOpt's two eightbytes are returned in two
// integer registers by the platform C ABI.
pub(crate) type JitEntry =
    unsafe extern "C" fn(*const VariableOpt<'static>, *const Regex) -> VariableOpt<'static>;

/// An expression compiled to native machine code.
///
/// This object owns the JIT module containing its executable memory and every
/// resource referenced by the generated code.
pub struct CompiledFn {
    pub(crate) entry: JitEntry,
    pub(crate) _module: JITModule,
    // Typed string-literal descriptors borrow their bytes from these values.
    pub(crate) _string_literals: Box<[Arc<str>]>,
    // Generated code selects a compiled regex by its index in this array.
    pub(crate) regexes: Box<[Regex]>,
    // Each regex call site owns stable storage for the StringRef descriptor it
    // returns. UnsafeCell makes the mutation performed by the Rust helper
    // explicit and prevents CompiledFn from being shared between threads.
    pub(crate) _regex_match_results: Box<[UnsafeCell<StringRef<'static>>]>,
    /// Input slots in the exact order expected by [`CompiledFn::call`].
    pub inputs: Vec<TypedVariable>,
    // Generated code embeds addresses of StringRef descriptors in this AST.
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
    /// contain the union member corresponding to that variable's type. The
    /// payload of an absent slot is ignored, and any referenced strings must
    /// remain alive for the duration of this call. A string result descriptor
    /// remains valid until the next call to this `CompiledFn`.
    ///
    /// The result cannot outlive the compiled function nor the passed
    /// arguments's lifetime.
    pub unsafe fn call<'args, 'compiled, 'output>(
        &'compiled self,
        args: &[VariableOpt<'args>],
    ) -> VariableOpt<'output>
    where
        'args: 'output,
        'compiled: 'output,
    {
        debug_assert_eq!(args.len(), self.inputs.len());
        // The JIT ABI erases Rust lifetimes. Internally it uses `'static` as a
        // placeholder; the public bounds above narrow the returned value to a
        // lifetime outlived by both possible sources of string data.
        let result = unsafe {
            (self.entry)(
                args.as_ptr().cast::<VariableOpt<'static>>(),
                self.regexes.as_ptr(),
            )
        };
        // SAFETY: `VariableOpt` has the same representation for every
        // lifetime. The call contract guarantees live inputs, and both input
        // and compiled-function lifetimes outlive `'output`.
        unsafe { mem::transmute::<VariableOpt<'static>, VariableOpt<'output>>(result) }
    }
}
