mod add;
mod and;
mod eq;
mod is_not_null;
mod is_null;
mod lower;
mod multiply;
mod native_function;
mod not;
mod or;
mod regexp_extract;
mod subtract;

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

pub(crate) use self::add::AddFnCall;
pub(crate) use self::and::AndFnCall;
pub(crate) use self::eq::EqFnCall;
pub(crate) use self::is_not_null::IsNotNullFnCall;
pub(crate) use self::is_null::IsNullFnCall;
pub(crate) use self::lower::LowerFnCall;
pub(crate) use self::multiply::MultiplyFnCall;
pub(crate) use self::native_function::{
    NativeFunctions, declare_native_functions, register_jit_symbols,
};
pub(crate) use self::not::NotFnCall;
pub(crate) use self::or::OrFnCall;
pub(crate) use self::regexp_extract::RegexpExtractFnCall;
pub(crate) use self::subtract::SubtractFnCall;
use crate::ast::{InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr};
use crate::types::VarType;

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    /// Conjoins one or more booleans with strict null propagation.
    And,
    /// Adds zero or more numerical expressions.
    Add,
    /// Compares two expressions for value equality.
    Eq,
    /// Tests whether an expression produced a present value.
    IsNotNull,
    /// Tests whether an expression produced an absent value.
    IsNull,
    /// Constructs the Unicode-lowercase form of a string.
    Lower,
    /// Multiplies two numeric arguments.
    Multiply,
    /// Negates a boolean, treating an absent input as false.
    Not,
    /// Disjoins one or more booleans, remaining present if any operand is present.
    Or,
    /// Extracts a capture group from a string using a constant regular expression.
    RegexpExtract,
    /// Subtracts the second numeric argument from the first.
    Subtract,
}

impl Function {
    pub(crate) fn call_with_types(
        self,
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        match self {
            Function::And => <AndFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Add => <AddFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Eq => <EqFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::IsNotNull => {
                <IsNotNullFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::IsNull => {
                <IsNullFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Lower => {
                <LowerFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Multiply => {
                <MultiplyFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Not => <NotFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Or => <OrFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Subtract => {
                <SubtractFnCall as FnCall>::call_with_types(args, target_type_set, context)
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
            Function::And => <AndFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Add => <AddFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Eq => <EqFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::IsNotNull => {
                <IsNotNullFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::IsNull => {
                <IsNullFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Lower => {
                <LowerFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Multiply => {
                <MultiplyFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Not => <NotFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Or => <OrFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Subtract => {
                <SubtractFnCall as FnCall>::infer_types(args, target_type, inferred_types)
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
    And(AndFnCall),
    Add(AddFnCall),
    Eq(EqFnCall),
    IsNull(IsNullFnCall),
    IsNotNull(IsNotNullFnCall),
    Lower(LowerFnCall),
    Multiply(MultiplyFnCall),
    Not(NotFnCall),
    Or(OrFnCall),
    RegexpExtract(RegexpExtractFnCall),
    Subtract(SubtractFnCall),
}

impl FnCallEnum {
    pub(crate) fn args_mut(&mut self) -> &mut [TypedExpr] {
        match self {
            FnCallEnum::And(call) => call.args_mut(),
            FnCallEnum::Add(call) => call.args_mut(),
            FnCallEnum::Eq(call) => call.args_mut(),
            FnCallEnum::IsNull(call) => call.args_mut(),
            FnCallEnum::IsNotNull(call) => call.args_mut(),
            FnCallEnum::Lower(call) => call.args_mut(),
            FnCallEnum::Multiply(call) => call.args_mut(),
            FnCallEnum::Not(call) => call.args_mut(),
            FnCallEnum::Or(call) => call.args_mut(),
            FnCallEnum::RegexpExtract(call) => call.args_mut(),
            FnCallEnum::Subtract(call) => call.args_mut(),
        }
    }

    /// Produce CraneLift IR for the given function call.
    pub(crate) fn lower(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        match self {
            FnCallEnum::And(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Add(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Eq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IsNull(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IsNotNull(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Lower(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Multiply(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Not(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Or(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::RegexpExtract(call) => {
                call.emit_cranelift_ir(return_type, context, builder)
            }
            FnCallEnum::Subtract(call) => call.emit_cranelift_ir(return_type, context, builder),
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
    /// arguments through `context`, and stores any compilation resources on the typed call.
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
    ) -> Result<LoweredValue, CompileError>;
}
