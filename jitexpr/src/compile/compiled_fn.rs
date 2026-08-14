use std::cell::UnsafeCell;
use std::sync::Arc;

use cranelift_jit::JITModule;
use regex::Regex;

use super::{TypedExpr, TypedVariable};
use crate::types::{StringRef, VariableValue};

pub(crate) type JitEntry =
    unsafe extern "C" fn(*const VariableValue, *mut VariableValue, *const Regex);

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
    pub(crate) _regex_match_results: Box<[UnsafeCell<StringRef>]>,
    pub(crate) input_vars: Vec<TypedVariable>,
    // Generated code embeds addresses of StringRef descriptors in this AST.
    pub(crate) _typed_expr: Box<TypedExpr>,
}

impl CompiledFn {
    /// Evaluate the compiled expression.
    ///
    /// # Safety
    ///
    /// `args` must follow `input_vars` exactly: every slot must contain the
    /// union member corresponding to that variable's type. `result` must be a
    /// valid writable slot and any referenced strings must remain alive for
    /// the duration of this call. A string result descriptor remains valid
    /// until the next call to this `CompiledFn`.
    pub unsafe fn call(&self, args: &[VariableValue], result: &mut VariableValue) {
        debug_assert_eq!(args.len(), self.input_vars.len());
        // SAFETY: Guaranteed by the caller.
        unsafe { (self.entry)(args.as_ptr(), result, self.regexes.as_ptr()) };
    }
}
