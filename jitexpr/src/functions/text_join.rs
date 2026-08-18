//! `TEXT_JOIN` is production's second name for the `CONCAT` string-joining operation.
//!
//! It accepts `TEXT_JOIN(delimiter, ignore_empty, value1, value2, ...)`, with at least four total
//! arguments. The first two arguments must be string literals. Only a case-insensitive `"true"`
//! enables empty-value skipping; other flag strings mean false. Any null scalar value makes the
//! result null. Delimiters are inserted only after nonempty output bytes have been written, so a
//! leading empty value does not produce a leading delimiter even when empty values are retained.
//!
//! dd-go dispatches `TEXT_JOIN` and `CONCAT` to the same kernel. Its multivalued positional-zip
//! behavior is outside jitexpr's scalar model. Constructed bytes use the fixed call arena and
//! arena exhaustion returns null.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

use super::concat::{self, JoinArguments};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TextJoinFnCall {
    arguments: JoinArguments,
}

impl FnCall for TextJoinFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        concat::infer_join_types(Function::TextJoin, args, target_type, inferred_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        let Some(arguments) = concat::apply_join_types("TEXT_JOIN", args, context)? else {
            return Ok(TypedExpr::none());
        };
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(TextJoinFnCall { arguments }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        self.arguments.args_mut()
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Str);
        self.arguments.emit_cranelift_ir(context, builder)
    }
}

impl From<TextJoinFnCall> for FnCallEnum {
    fn from(call: TextJoinFnCall) -> Self {
        FnCallEnum::TextJoin(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<String> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: These expressions have no inputs and return nullable strings.
        unsafe { compiled.call(&[]).as_str().map(str::to_owned) }
    }

    #[test]
    fn test_requires_four_or_more_string_arguments() {
        let expression = deserialize(r#"(TEXT_JOIN "," "false" left right)"#).unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("left"), Some(&InferredTypeSet::STRING));
        assert_eq!(inferred_types.get("right"), Some(&InferredTypeSet::STRING));

        let expression = deserialize(r#"(TEXT_JOIN "," "false" one)"#).unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::TextJoin,
                expected: 4,
                ..
            })
        ));
    }

    #[test]
    fn test_matches_concat_scalar_behavior() {
        assert_eq!(
            eval(r#"(TEXT_JOIN " / " "TRUE" "one" "" "two")"#).as_deref(),
            Some("one / two")
        );
        assert_eq!(
            eval(r#"(TEXT_JOIN "," "false" "" "two")"#).as_deref(),
            Some("two")
        );
        assert_eq!(
            eval(r#"(TEXT_JOIN "," "false" "two" "")"#).as_deref(),
            Some("two,")
        );
    }

    #[test]
    fn test_runtime_null_is_strict() {
        let expression = deserialize(r#"(TEXT_JOIN "" "false" left right)"#).unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::some("right")])
                    .as_str()
            },
            None
        );
    }
}
