mod add;
mod is_null;
mod native_function;
mod regexp_extract;

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

pub(crate) use self::add::AddFnCall;
pub(crate) use self::is_null::IsNullFnCall;
pub(crate) use self::native_function::{
    NativeFunctions, declare_native_functions, register_jit_symbols,
};
pub(crate) use self::regexp_extract::RegexpExtractFnCall;
use crate::ast::{InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr};
use crate::types::VarType;

/// A function supported by the first expression-language milestone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Function {
    /// Adds zero or more numerical expressions.
    Add,
    /// Tests whether an expression produced an absent value.
    IsNull,
    /// Extracts a capture group from a string using a constant regular expression.
    RegexpExtract,
}

impl Function {
    pub(crate) fn call(self, args: Vec<UntypedExpr>) -> Result<UntypedExpr, InvalidFunctionCall> {
        match self {
            Function::Add => <AddFnCall as FnCall>::validate_args(&args)?,
            Function::IsNull => <IsNullFnCall as FnCall>::validate_args(&args)?,
            Function::RegexpExtract => <RegexpExtractFnCall as FnCall>::validate_args(&args)?,
        }

        Ok(UntypedExpr::Call {
            function: self,
            args,
        })
    }

    pub(crate) fn call_with_types(
        self,
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        match self {
            Function::Add => <AddFnCall as FnCall>::call_with_types(args, target_type_set, context),
            Function::IsNull => {
                <IsNullFnCall as FnCall>::call_with_types(args, target_type_set, context)
            }
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
            Function::IsNull => {
                <IsNullFnCall as FnCall>::infer_types(args, target_type, inferred_types)
            }
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
    IsNull(IsNullFnCall),
    RegexpExtract(RegexpExtractFnCall),
}

impl FnCallEnum {
    pub(crate) fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FnCallEnum::Add(call) => call.serialize(formatter),
            FnCallEnum::IsNull(call) => call.serialize(formatter),
            FnCallEnum::RegexpExtract(call) => call.serialize(formatter),
        }
    }

    pub(crate) fn args_mut(&mut self) -> &mut [TypedExpr] {
        match self {
            FnCallEnum::Add(call) => call.args_mut(),
            FnCallEnum::IsNull(call) => call.args_mut(),
            FnCallEnum::RegexpExtract(call) => call.args_mut(),
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
            FnCallEnum::Add(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::IsNull(call) => call.emit_cranelift_ir(return_type, context, builder),
            FnCallEnum::RegexpExtract(call) => {
                call.emit_cranelift_ir(return_type, context, builder)
            }
        }
    }
}

/// Error representing an invalid function call.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidFunctionCall {
    #[error("invalid number of arguments: expected {expected}, got {provided}")]
    InvalidNumberOfArguments {
        expected: ArgumentCount,
        provided: usize,
    },
    #[error("argument {argument} must be a {expected:?} literal")]
    ExpectedLiteral { argument: usize, expected: VarType },
    #[error("invalid value for argument {argument}: expected {expected}")]
    InvalidLiteralValue {
        argument: usize,
        expected: &'static str,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgumentCount {
    Any,
    Exactly(usize),
    AtLeast(usize),
    Between { min: usize, max: usize },
}

impl std::fmt::Display for ArgumentCount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ArgumentCount::Any => formatter.write_str("any number of arguments"),
            ArgumentCount::Exactly(1) => formatter.write_str("exactly 1 argument"),
            ArgumentCount::Exactly(count) => {
                write!(formatter, "exactly {count} arguments")
            }
            ArgumentCount::AtLeast(1) => formatter.write_str("at least 1 argument"),
            ArgumentCount::AtLeast(count) => {
                write!(formatter, "at least {count} arguments")
            }
            ArgumentCount::Between { min: 1, max: 1 } => formatter.write_str("exactly 1 argument"),
            ArgumentCount::Between { min, max } if min == max => {
                write!(formatter, "exactly {min} arguments")
            }
            ArgumentCount::Between { min, max } => {
                write!(formatter, "between {min} and {max} arguments")
            }
        }
    }
}

impl ArgumentCount {
    fn validate(self, args: &[UntypedExpr]) -> Result<(), InvalidFunctionCall> {
        let provided = args.len();
        let is_valid = match self {
            ArgumentCount::Any => true,
            ArgumentCount::Exactly(expected) => provided == expected,
            ArgumentCount::AtLeast(expected) => provided >= expected,
            ArgumentCount::Between { min, max } => (min..=max).contains(&provided),
        };
        if is_valid {
            Ok(())
        } else {
            Err(InvalidFunctionCall::InvalidNumberOfArguments {
                expected: self,
                provided,
            })
        }
    }
}

pub(crate) fn validate_literal(
    args: &[UntypedExpr],
    index: usize,
    expected: VarType,
    is_valid: impl FnOnce(&Literal) -> bool,
) -> Result<(), InvalidFunctionCall> {
    let Some(UntypedExpr::Literal(literal)) = args.get(index) else {
        return Err(InvalidFunctionCall::ExpectedLiteral {
            argument: index + 1,
            expected,
        });
    };
    if is_valid(literal) {
        Ok(())
    } else {
        Err(InvalidFunctionCall::ExpectedLiteral {
            argument: index + 1,
            expected,
        })
    }
}

/// Implements the type-inference, typed-AST, and lowering phases of a function call.
///
/// The static methods operate on an [`UntypedExpr`] call before a concrete call node exists.
/// Once [`FnCall::call_with_types`] has produced that node, [`FnCall::args_mut`] and
/// [`FnCall::lower`] operate on its typed representation.
pub(crate) trait FnCall: std::fmt::Debug + Into<FnCallEnum> {
    const ARG_COUNT: ArgumentCount;

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

    fn validate_args(args: &[UntypedExpr]) -> Result<(), InvalidFunctionCall> {
        Self::ARG_COUNT.validate(args)?;
        Ok(())
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::compile;

    fn call_error(function: Function, args: Vec<UntypedExpr>) -> InvalidFunctionCall {
        match UntypedExpr::call(function, args) {
            Ok(_) => panic!("expected the function call to be rejected"),
            Err(error) => error,
        }
    }

    #[test]
    fn test_argument_count_display() {
        assert_eq!(ArgumentCount::Any.to_string(), "any number of arguments");
        assert_eq!(ArgumentCount::Exactly(1).to_string(), "exactly 1 argument");
        assert_eq!(ArgumentCount::Exactly(2).to_string(), "exactly 2 arguments");
        assert_eq!(ArgumentCount::AtLeast(1).to_string(), "at least 1 argument");
        assert_eq!(
            ArgumentCount::AtLeast(2).to_string(),
            "at least 2 arguments"
        );
        assert_eq!(
            ArgumentCount::Between { min: 2, max: 3 }.to_string(),
            "between 2 and 3 arguments"
        );
    }

    #[test]
    fn test_argument_count_validation() {
        assert_eq!(
            call_error(Function::IsNull, Vec::new()),
            InvalidFunctionCall::InvalidNumberOfArguments {
                expected: ArgumentCount::Exactly(1),
                provided: 0,
            }
        );
        assert_eq!(
            ArgumentCount::AtLeast(1).validate(&[]).unwrap_err(),
            InvalidFunctionCall::InvalidNumberOfArguments {
                expected: ArgumentCount::AtLeast(1),
                provided: 0,
            }
        );
        assert_eq!(
            call_error(Function::RegexpExtract, Vec::new()),
            InvalidFunctionCall::InvalidNumberOfArguments {
                expected: ArgumentCount::Between { min: 2, max: 3 },
                provided: 0,
            }
        );
        assert!(UntypedExpr::call(Function::Add, Vec::new()).is_ok());
    }

    #[test]
    fn test_literal_argument_validation() {
        assert_eq!(
            call_error(
                Function::RegexpExtract,
                vec![
                    UntypedExpr::variable("input"),
                    UntypedExpr::variable("pattern")
                ],
            ),
            InvalidFunctionCall::ExpectedLiteral {
                argument: 2,
                expected: VarType::Str,
            }
        );
        assert_eq!(
            call_error(
                Function::RegexpExtract,
                vec![
                    UntypedExpr::variable("input"),
                    UntypedExpr::literal("pattern"),
                    UntypedExpr::literal(1i64),
                ],
            ),
            InvalidFunctionCall::ExpectedLiteral {
                argument: 3,
                expected: VarType::U64,
            }
        );
    }

    #[test]
    fn test_typed_construction_validates_unchecked_ast() {
        let expression = Function::IsNull.call_untyped_expr(Vec::new());
        let error = match compile(&expression, &HashMap::new()) {
            Ok(_) => panic!("expected compilation to reject the unchecked AST"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            CompileError::InvalidArguments(InvalidFunctionCall::InvalidNumberOfArguments {
                expected: ArgumentCount::Exactly(1),
                provided: 0,
            })
        ));

        let expression = Function::RegexpExtract.call_untyped_expr(vec![
            UntypedExpr::variable("input"),
            UntypedExpr::variable("pattern"),
        ]);
        let variable_types = HashMap::from([("input", VarType::Str), ("pattern", VarType::Str)]);
        let error = match compile(&expression, &variable_types) {
            Ok(_) => panic!("expected compilation to reject the non-literal pattern"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            CompileError::InvalidArguments(InvalidFunctionCall::ExpectedLiteral {
                argument: 2,
                expected: VarType::Str,
            })
        ));
    }
}
