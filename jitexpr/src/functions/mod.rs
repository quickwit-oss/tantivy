mod add;
mod eq;
mod native_function;
mod regexp_extract;

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

pub(crate) use self::add::AddFnCall;
pub(crate) use self::eq::EqFnCall;
pub(crate) use self::native_function::{
    NativeFunctions, declare_native_functions, register_jit_symbols,
};
pub(crate) use self::regexp_extract::RegexpExtractFnCall;
use crate::ast::{InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweringContext, TypedExpr};
use crate::types::VarType;

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    /// Adds zero or more numerical expressions.
    Add,
    /// Compares two expressions for value equality.
    Eq,
    /// Extracts a capture group from a string using a constant regular expression.
    RegexpExtract,
}

impl Function {
    pub(crate) fn call_with_types(
        self,
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        match self {
            Function::Add => <AddFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Eq => <EqFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
        }
    }

    pub(crate) fn infer_types<'a>(
        self,
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        match self {
            Function::Add => <AddFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Eq => <EqFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
        }
    }

    pub fn call_untyped_expr(self, args: Vec<UntypedExpr>) -> UntypedExpr {
        UntypedExpr::Call {
            function: self,
            args,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FnCallEnum {
    Add(AddFnCall),
    Eq(EqFnCall),
    RegexpExtract(RegexpExtractFnCall),
}

impl FnCallEnum {
    pub(crate) fn args_mut(&mut self) -> &mut [TypedExpr] {
        match self {
            FnCallEnum::Add(call) => call.args_mut(),
            FnCallEnum::Eq(call) => call.args_mut(),
            FnCallEnum::RegexpExtract(call) => call.args_mut(),
        }
    }

    /// Produce CraneLift IR for the given function call.
    pub(crate) fn lower(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<cranelift::codegen::ir::Value, CompileError> {
        match self {
            FnCallEnum::Add(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Eq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::RegexpExtract(call) => {
                call.emit_cranelift_ir(return_type, context, builder)
            }
        }
    }
}

/// Implements the type-inference, typed-AST, and lowering phases of a function call.
///
/// The static methods operate on an [`UntypedExpr`] call before a concrete call node exists.
/// Once [`FnCall::call_with_types`] has produced that node, [`FnCall::args_mut`] and
/// [`FnCall::lower`] operate on its typed representation.
pub(crate) trait FnCall: std::fmt::Debug + Into<FnCallEnum> {
    /// Constrains the call and its arguments to the types accepted by its parent expression.
    ///
    /// Implementations validate their signature, recursively infer every argument, update
    /// `inferred_types` with the accepted types for variables, and return the possible result
    /// types that remain after intersecting with `target_type`.
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError>
    where
        Self: Sized;

    /// Builds the typed call after concrete variable types have been supplied.
    ///
    /// `target_type_set` communicates the result types set accepted by the parent call. The
    /// implementation selects a concrete result type, applies compatible target types to its
    /// arguments through `context`, and registers any compilation resources owned by the call.
    ///
    /// The type of the returned is given to the caller in the TypedExpr object.
    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError>
    where
        Self: Sized;

    /// Returns the typed child expressions that participate in recursive AST passes.
    ///
    /// This is only used, to assign and deduplicate variable input slots. Compile-time
    /// configuration stored directly on a call does not need to be returned.
    ///
    /// Today this is only used as a cheap visitor to allocate variable ids.
    fn args_mut(&mut self) -> &mut [TypedExpr];

    /// Emits Cranelift IR for an already typed call and returns its result SSA value.
    ///
    /// `return_type` is the concrete type selected during typed-AST construction. Implementations
    /// lower child expressions through `context` and append their own instructions to `builder`.
    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<cranelift::codegen::ir::Value, CompileError>;
}
