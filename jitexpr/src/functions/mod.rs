mod abs;
mod add;
mod and;
mod ceil;
mod comparison;
mod concat;
mod divide;
mod eq;
mod floor;
mod gt;
mod gt_eq;
mod if_fn;
mod int_mod;
mod is_not_null;
mod is_null;
mod left;
mod lower;
mod lt;
mod lt_eq;
mod max;
mod min;
mod multiply;
mod native_function;
mod neq;
mod not;
mod or;
mod pow;
mod regexp_extract;
mod regexp_like;
mod right;
mod round;
mod split_after;
mod split_before;
mod sqrt;
mod substring;
mod substring_count;
mod subtract;
mod text_join;
mod trim;
mod upper;

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

pub(crate) use self::abs::AbsFnCall;
pub(crate) use self::add::AddFnCall;
pub(crate) use self::and::AndFnCall;
pub(crate) use self::ceil::CeilFnCall;
pub(crate) use self::concat::ConcatFnCall;
pub(crate) use self::divide::DivideFnCall;
pub(crate) use self::eq::EqFnCall;
pub(crate) use self::floor::FloorFnCall;
pub(crate) use self::gt::GtFnCall;
pub(crate) use self::gt_eq::GtEqFnCall;
pub(crate) use self::if_fn::IfFnCall;
pub(crate) use self::int_mod::IntModFnCall;
pub(crate) use self::is_not_null::IsNotNullFnCall;
pub(crate) use self::is_null::IsNullFnCall;
pub(crate) use self::left::LeftFnCall;
pub(crate) use self::lower::LowerFnCall;
pub(crate) use self::lt::LtFnCall;
pub(crate) use self::lt_eq::LtEqFnCall;
pub(crate) use self::max::MaxFnCall;
pub(crate) use self::min::MinFnCall;
pub(crate) use self::multiply::MultiplyFnCall;
pub(crate) use self::native_function::{
    NativeFunctions, declare_native_functions, register_jit_symbols,
};
pub(crate) use self::neq::NeqFnCall;
pub(crate) use self::not::NotFnCall;
pub(crate) use self::or::OrFnCall;
pub(crate) use self::pow::PowFnCall;
pub(crate) use self::regexp_extract::RegexpExtractFnCall;
pub(crate) use self::regexp_like::RegexpLikeFnCall;
pub(crate) use self::right::RightFnCall;
pub(crate) use self::round::RoundFnCall;
pub(crate) use self::split_after::SplitAfterFnCall;
pub(crate) use self::split_before::SplitBeforeFnCall;
pub(crate) use self::sqrt::SqrtFnCall;
pub(crate) use self::substring::SubstringFnCall;
pub(crate) use self::substring_count::SubstringCountFnCall;
pub(crate) use self::subtract::SubtractFnCall;
pub(crate) use self::text_join::TextJoinFnCall;
pub(crate) use self::trim::TrimFnCall;
pub(crate) use self::upper::UpperFnCall;
use crate::ast::{InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr};
use crate::types::VarType;

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    /// Computes the absolute value of a number.
    Abs,
    /// Conjoins one or more booleans with strict null propagation.
    And,
    /// Returns the least integer greater than or equal to a number.
    Ceil,
    /// Joins strings using a literal delimiter and empty-value policy.
    Concat,
    /// Adds zero or more numerical expressions.
    Add,
    /// Divides two numeric arguments using floating-point arithmetic.
    Divide,
    /// Compares two expressions for value equality.
    Eq,
    /// Returns the greatest integer less than or equal to a number.
    Floor,
    /// Tests whether the first ordered value is greater than the second.
    Gt,
    /// Tests whether the first ordered value is greater than or equal to the second.
    GtEq,
    /// Selects one of two values using a boolean condition.
    If,
    /// Computes floating-point modulo with the divisor's sign.
    IntMod,
    /// Returns the first requested number of bytes from a string.
    Left,
    /// Tests whether the first ordered value is less than the second.
    Lt,
    /// Tests whether the first ordered value is less than or equal to the second.
    LtEq,
    /// Tests whether an expression produced a present value.
    IsNotNull,
    /// Tests whether an expression produced an absent value.
    IsNull,
    /// Constructs the Unicode-lowercase form of a string.
    Lower,
    /// Returns the greatest of one or more numbers.
    Max,
    /// Returns the least of one or more numbers.
    Min,
    /// Multiplies two numeric arguments.
    Multiply,
    /// Tests inequality using `NOT(EQ(...))` null semantics.
    Neq,
    /// Negates a boolean, treating an absent input as false.
    Not,
    /// Disjoins one or more booleans, remaining present if any operand is present.
    Or,
    /// Raises a numeric base to a numeric exponent and returns a float.
    Pow,
    /// Returns the floating-point square root of a number, or null for a NaN result.
    Sqrt,
    /// Extracts a capture group from a string using a constant regular expression.
    RegexpExtract,
    /// Tests whether a constant regular expression matches a string.
    RegexpLike,
    /// Returns the last requested number of bytes from a string.
    Right,
    /// Rounds a number to a constant decimal precision.
    Round,
    /// Returns the suffix after a selected occurrence of a literal separator.
    SplitAfter,
    /// Returns the prefix before a selected occurrence of a literal separator.
    SplitBefore,
    /// Subtracts the second numeric argument from the first.
    Subtract,
    /// Returns a byte-indexed string slice.
    Substring,
    /// Counts non-overlapping occurrences of one string in another.
    SubstringCount,
    /// Joins strings with the same semantics as `CONCAT`.
    TextJoin,
    /// Removes a whole delimiter from selected ends of a string.
    Trim,
    /// Converts a string to Unicode uppercase.
    Upper,
}

impl Function {
    pub(crate) fn call_with_types(
        self,
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        match self {
            Function::Abs => <AbsFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::And => <AndFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Ceil => {
                <CeilFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Concat => {
                <ConcatFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Add => <AddFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Divide => {
                <DivideFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Eq => <EqFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Floor => {
                <FloorFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Gt => <GtFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::GtEq => {
                <GtEqFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::If => <IfFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::IntMod => {
                <IntModFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Left => {
                <LeftFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Lt => <LtFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::LtEq => {
                <LtEqFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::IsNotNull => {
                <IsNotNullFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::IsNull => {
                <IsNullFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Lower => {
                <LowerFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Max => <MaxFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Min => <MinFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Multiply => {
                <MultiplyFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Neq => <NeqFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Not => <NotFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Or => <OrFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Pow => <PowFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::Sqrt => {
                <SqrtFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::RegexpLike => {
                <RegexpLikeFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Right => {
                <RightFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Round => {
                <RoundFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::SplitAfter => {
                <SplitAfterFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::SplitBefore => {
                <SplitBeforeFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Subtract => {
                <SubtractFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Substring => {
                <SubstringFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::SubstringCount => {
                <SubstringCountFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::TextJoin => {
                <TextJoinFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Trim => {
                <TrimFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
            Function::Upper => {
                <UpperFnCall as FnCall>::call_with_types(args, target_type_set, context)
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
            Function::Abs => <AbsFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::And => <AndFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Ceil => {
                <CeilFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Concat => {
                <ConcatFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Add => <AddFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Divide => {
                <DivideFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Eq => <EqFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Floor => {
                <FloorFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Gt => <GtFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::GtEq => {
                <GtEqFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::If => <IfFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::IntMod => {
                <IntModFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Left => {
                <LeftFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Lt => <LtFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::LtEq => {
                <LtEqFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::IsNotNull => {
                <IsNotNullFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::IsNull => {
                <IsNullFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Lower => {
                <LowerFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Max => <MaxFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Min => <MinFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Multiply => {
                <MultiplyFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Neq => <NeqFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Not => <NotFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Or => <OrFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Pow => <PowFnCall as FnCall>::infer_types(args, target_type, inferred_types),
            Function::Sqrt => {
                <SqrtFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::RegexpExtract => {
                <RegexpExtractFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::RegexpLike => {
                <RegexpLikeFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Right => {
                <RightFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Round => {
                <RoundFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::SplitAfter => {
                <SplitAfterFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::SplitBefore => {
                <SplitBeforeFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Subtract => {
                <SubtractFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Substring => {
                <SubstringFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::SubstringCount => {
                <SubstringCountFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::TextJoin => {
                <TextJoinFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Trim => {
                <TrimFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
            Function::Upper => {
                <UpperFnCall as FnCall>::infer_types(args, target_type, inferred_types)
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
    Abs(AbsFnCall),
    And(AndFnCall),
    Ceil(CeilFnCall),
    Concat(ConcatFnCall),
    Add(AddFnCall),
    Divide(DivideFnCall),
    Eq(EqFnCall),
    Floor(FloorFnCall),
    Gt(GtFnCall),
    GtEq(GtEqFnCall),
    If(IfFnCall),
    IntMod(IntModFnCall),
    Left(LeftFnCall),
    Lt(LtFnCall),
    LtEq(LtEqFnCall),
    IsNull(IsNullFnCall),
    IsNotNull(IsNotNullFnCall),
    Lower(LowerFnCall),
    Max(MaxFnCall),
    Min(MinFnCall),
    Multiply(MultiplyFnCall),
    Neq(NeqFnCall),
    Not(NotFnCall),
    Or(OrFnCall),
    Pow(PowFnCall),
    Sqrt(SqrtFnCall),
    RegexpExtract(RegexpExtractFnCall),
    RegexpLike(RegexpLikeFnCall),
    Right(RightFnCall),
    Round(RoundFnCall),
    SplitAfter(SplitAfterFnCall),
    SplitBefore(SplitBeforeFnCall),
    Subtract(SubtractFnCall),
    Substring(SubstringFnCall),
    SubstringCount(SubstringCountFnCall),
    TextJoin(TextJoinFnCall),
    Trim(TrimFnCall),
    Upper(UpperFnCall),
}

impl FnCallEnum {
    pub(crate) fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FnCallEnum::Abs(call) => call.serialize(formatter),
            FnCallEnum::And(call) => call.serialize(formatter),
            FnCallEnum::Ceil(call) => call.serialize(formatter),
            FnCallEnum::Concat(call) => call.serialize(formatter),
            FnCallEnum::Add(call) => call.serialize(formatter),
            FnCallEnum::Divide(call) => call.serialize(formatter),
            FnCallEnum::Eq(call) => call.serialize(formatter),
            FnCallEnum::Floor(call) => call.serialize(formatter),
            FnCallEnum::Gt(call) => call.serialize(formatter),
            FnCallEnum::GtEq(call) => call.serialize(formatter),
            FnCallEnum::If(call) => call.serialize(formatter),
            FnCallEnum::IntMod(call) => call.serialize(formatter),
            FnCallEnum::Left(call) => call.serialize(formatter),
            FnCallEnum::Lt(call) => call.serialize(formatter),
            FnCallEnum::LtEq(call) => call.serialize(formatter),
            FnCallEnum::IsNull(call) => call.serialize(formatter),
            FnCallEnum::IsNotNull(call) => call.serialize(formatter),
            FnCallEnum::Lower(call) => call.serialize(formatter),
            FnCallEnum::Max(call) => call.serialize(formatter),
            FnCallEnum::Min(call) => call.serialize(formatter),
            FnCallEnum::Multiply(call) => call.serialize(formatter),
            FnCallEnum::Neq(call) => call.serialize(formatter),
            FnCallEnum::Not(call) => call.serialize(formatter),
            FnCallEnum::Or(call) => call.serialize(formatter),
            FnCallEnum::Pow(call) => call.serialize(formatter),
            FnCallEnum::Sqrt(call) => call.serialize(formatter),
            FnCallEnum::RegexpExtract(call) => call.serialize(formatter),
            FnCallEnum::RegexpLike(call) => call.serialize(formatter),
            FnCallEnum::Right(call) => call.serialize(formatter),
            FnCallEnum::Round(call) => call.serialize(formatter),
            FnCallEnum::SplitAfter(call) => call.serialize(formatter),
            FnCallEnum::SplitBefore(call) => call.serialize(formatter),
            FnCallEnum::Subtract(call) => call.serialize(formatter),
            FnCallEnum::Substring(call) => call.serialize(formatter),
            FnCallEnum::SubstringCount(call) => call.serialize(formatter),
            FnCallEnum::TextJoin(call) => call.serialize(formatter),
            FnCallEnum::Trim(call) => call.serialize(formatter),
            FnCallEnum::Upper(call) => call.serialize(formatter),
        }
    }

    pub(crate) fn args_mut(&mut self) -> &mut [TypedExpr] {
        match self {
            FnCallEnum::Abs(call) => call.args_mut(),
            FnCallEnum::And(call) => call.args_mut(),
            FnCallEnum::Ceil(call) => call.args_mut(),
            FnCallEnum::Concat(call) => call.args_mut(),
            FnCallEnum::Add(call) => call.args_mut(),
            FnCallEnum::Divide(call) => call.args_mut(),
            FnCallEnum::Eq(call) => call.args_mut(),
            FnCallEnum::Floor(call) => call.args_mut(),
            FnCallEnum::Gt(call) => call.args_mut(),
            FnCallEnum::GtEq(call) => call.args_mut(),
            FnCallEnum::If(call) => call.args_mut(),
            FnCallEnum::IntMod(call) => call.args_mut(),
            FnCallEnum::Left(call) => call.args_mut(),
            FnCallEnum::Lt(call) => call.args_mut(),
            FnCallEnum::LtEq(call) => call.args_mut(),
            FnCallEnum::IsNull(call) => call.args_mut(),
            FnCallEnum::IsNotNull(call) => call.args_mut(),
            FnCallEnum::Lower(call) => call.args_mut(),
            FnCallEnum::Max(call) => call.args_mut(),
            FnCallEnum::Min(call) => call.args_mut(),
            FnCallEnum::Multiply(call) => call.args_mut(),
            FnCallEnum::Neq(call) => call.args_mut(),
            FnCallEnum::Not(call) => call.args_mut(),
            FnCallEnum::Or(call) => call.args_mut(),
            FnCallEnum::Pow(call) => call.args_mut(),
            FnCallEnum::Sqrt(call) => call.args_mut(),
            FnCallEnum::RegexpExtract(call) => call.args_mut(),
            FnCallEnum::RegexpLike(call) => call.args_mut(),
            FnCallEnum::Right(call) => call.args_mut(),
            FnCallEnum::Round(call) => call.args_mut(),
            FnCallEnum::SplitAfter(call) => call.args_mut(),
            FnCallEnum::SplitBefore(call) => call.args_mut(),
            FnCallEnum::Subtract(call) => call.args_mut(),
            FnCallEnum::Substring(call) => call.args_mut(),
            FnCallEnum::SubstringCount(call) => call.args_mut(),
            FnCallEnum::TextJoin(call) => call.args_mut(),
            FnCallEnum::Trim(call) => call.args_mut(),
            FnCallEnum::Upper(call) => call.args_mut(),
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
            FnCallEnum::Abs(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::And(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Ceil(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Concat(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Add(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Divide(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Eq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Floor(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Gt(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::GtEq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::If(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IntMod(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Left(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Lt(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::LtEq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IsNull(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IsNotNull(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Lower(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Max(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Min(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Multiply(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Neq(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Not(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Or(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Pow(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Sqrt(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::RegexpExtract(call) => {
                call.emit_cranelift_ir(return_type, context, builder)
            }
            FnCallEnum::RegexpLike(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Right(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Round(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::SplitAfter(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::SplitBefore(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Subtract(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Substring(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::SubstringCount(call) => {
                call.emit_cranelift_ir(return_type, context, builder)
            }
            FnCallEnum::TextJoin(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Trim(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::Upper(call) => call.emit_cranelift_ir(return_type, context, builder),
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

    /// Serializes the function name and its normalized typed arguments.
    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result;

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
