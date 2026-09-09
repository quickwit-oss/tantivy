use std::ops::Deref;
use std::sync::Arc;

use cranelift_jit::JITModule;

use super::{StringArena, TypedExpr, TypedVariable};
use crate::types::{VarType, VariableValue};

#[cfg(not(any(
    all(target_arch = "x86_64", not(target_os = "windows")),
    target_arch = "aarch64"
)))]
// Windows is not supported because apparently returning more than one 64 bits word throught
// registers is not supported by its ABI.
compile_error!(
    "the direct VariableValue JIT return ABI is only implemented for x86-64 System V and AArch64"
);

// On the supported targets, VariableValue's two eightbytes are returned in two
// integer registers by the platform C ABI. The lifetime is selected by
// CompiledFn::call so that it is bounded by all possible sources of strings.
// This is a Rust-to-JIT boundary whose VariableValue layout is asserted in
// types.rs, not an interface intended for C callers.
#[allow(improper_ctypes_definitions)]
pub(crate) type JitEntry =
    for<'a> unsafe extern "C" fn(*const VariableValue<'a>, *mut StringArena) -> VariableValue<'a>;

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

// `JITModule` is not `Sync` because it supports lazily looking up symbols through
// interior mutability. A `CompiledFn` only retains a finalized module to keep its
// executable allocation alive and never invokes those mutable APIs. Its entry
// point and the immutable resources referenced by the generated code can be
// called concurrently when each caller supplies a distinct `StringArena`.
unsafe impl Sync for CompiledFn {}

impl CompiledFn {
    /// Returns the concrete result type selected during compilation.
    pub fn result_type(&self) -> VarType {
        self._typed_expr.return_type
    }

    /// Creates an evaluation context with a private string arena.
    pub fn context(self: &Arc<Self>) -> CompiledFnCtx {
        CompiledFnCtx::new(Arc::clone(self))
    }

    /// Evaluates the compiled expression using the supplied string arena.
    ///
    /// The mutable arena borrow prevents another evaluation from clearing the
    /// arena while an arena-backed result from this call is still live.
    ///
    /// # Safety
    ///
    /// `args` must follow [`CompiledFn::inputs`] exactly: every present slot must
    /// contain the union member corresponding to that variable's type. Absent
    /// slots must use [`VariableValue::none`], and any borrowed strings must
    /// remain alive for the duration of this call.
    ///
    /// The result cannot outlive the compiled function, the string arena, or
    /// the passed arguments' lifetime.
    #[inline(always)]
    pub unsafe fn call<'args, 'compiled, 'arena, 'output>(
        &'compiled self,
        args: &[VariableValue<'args>],
        string_arena: &'arena mut StringArena,
    ) -> VariableValue<'output>
    where
        'args: 'output,
        'compiled: 'output,
        'arena: 'output,
    {
        debug_assert_eq!(args.len(), self.inputs.len());
        let args: &[VariableValue<'output>] = args;
        let string_arena = &raw mut *string_arena;
        // SAFETY: Guaranteed by the caller. The input, compiled-function, and
        // arena lifetimes outlive the lifetime selected for the returned value.
        unsafe { (self.entry)(args.as_ptr(), string_arena) }
    }
}

/// Per-caller mutable state used to evaluate a shared [`CompiledFn`].
pub struct CompiledFnCtx {
    compiled_fn: Arc<CompiledFn>,
    pub(crate) string_arena: StringArena,
}

impl CompiledFnCtx {
    /// Creates an evaluation context for `compiled_fn`.
    pub fn new(compiled_fn: Arc<CompiledFn>) -> Self {
        Self {
            compiled_fn,
            string_arena: StringArena::new(),
        }
    }

    /// Evaluates the compiled expression using this context's string arena.
    ///
    /// The mutable borrow prevents another evaluation from clearing the string
    /// arena while an arena-backed result from this call is still live.
    ///
    /// # Safety
    ///
    /// `args` must follow [`CompiledFn::inputs`] exactly: every present slot must
    /// contain the union member corresponding to that variable's type. Absent
    /// slots must use [`VariableValue::none`], and any borrowed strings must
    /// remain alive for the duration of this call.
    ///
    /// The result cannot outlive this context nor the passed arguments'
    /// lifetime.
    #[inline(always)]
    pub unsafe fn call<'args, 'ctx, 'output>(
        &'ctx mut self,
        args: &[VariableValue<'args>],
    ) -> VariableValue<'output>
    where
        'args: 'output,
        'ctx: 'output,
    {
        // SAFETY: Guaranteed by the caller. The context owns both the compiled
        // function and arena for the lifetime selected for the returned value.
        unsafe { self.compiled_fn.call(args, &mut self.string_arena) }
    }

    /// Returns the shared compiled expression owned by this context.
    pub fn compiled_fn(&self) -> &Arc<CompiledFn> {
        &self.compiled_fn
    }
}

impl From<Arc<CompiledFn>> for CompiledFnCtx {
    fn from(compiled_fn: Arc<CompiledFn>) -> Self {
        Self::new(compiled_fn)
    }
}

impl Deref for CompiledFnCtx {
    type Target = CompiledFn;

    fn deref(&self) -> &Self::Target {
        &self.compiled_fn
    }
}
