//! `AND` combines one or more nullable boolean expressions.
//!
//! All arguments must be boolean. With present inputs it is ordinary conjunction. Null handling is
//! deliberately stricter than SQL: if any argument is absent, the result is absent even when
//! another argument is already `false`. Thus `FALSE AND NULL = NULL`. Null result rows carry a
//! false payload, matching dd-go's bitset representation.
//!
//! dd-go implements this by unioning the operands' null sets and clearing result bits at every null
//! row (`interpreter/vector.go`, `propagateNullsBoolean`). The production type checker accepts one
//! or more boolean operands. Evaluation order and short-circuiting are query-engine concerns; this
//! scalar implementation evaluates every child expression.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AndFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for AndFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::And,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.is_empty() {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::And,
                expected: 1,
                got: 0,
            });
        }

        for arg in args {
            crate::ast::infer_types_aux(arg, InferredTypeSet::BOOLEAN, inferred_types)?;
        }
        Ok(InferredTypeSet::BOOLEAN)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert!(!args.is_empty(), "expected at least 1 arg for AND");
        debug_assert!(target_type_set.contains(VarType::Bool));

        let args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::BOOLEAN))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(AndFnCall {
                args: args.into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let mut value = builder.ins().iconst(types::I8, 1);
        let mut is_present = value;

        for arg in &self.args {
            let lowered = context.compile_expr(arg, builder)?;
            is_present = builder.ins().band(is_present, lowered.is_present);
            if arg.return_type == VarType::None {
                value = builder.ins().iconst(types::I8, 0);
            } else {
                value = builder.ins().band(value, lowered.value);
            }
        }
        value = builder.ins().band(value, is_present);

        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<AndFnCall> for FnCallEnum {
    fn from(call: AndFnCall) -> Self {
        FnCallEnum::And(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<bool> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: These expressions have no runtime inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_infer_types_requires_boolean_arguments() {
        let expression = deserialize("(AND left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("left"), Some(&InferredTypeSet::BOOLEAN));
        assert_eq!(inferred_types.get("right"), Some(&InferredTypeSet::BOOLEAN));

        let expression = deserialize("(AND)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::And,
                expected: 1,
                got: 0,
            })
        ));
    }

    #[test]
    fn test_present_truth_table_and_variadic_inputs() {
        assert_eq!(eval("(AND true)"), Some(true));
        assert_eq!(eval("(AND true true true)"), Some(true));
        assert_eq!(eval("(AND true false true)"), Some(false));
        assert_eq!(eval("(AND false false)"), Some(false));
    }

    #[test]
    fn test_any_absent_argument_makes_result_absent() {
        assert_eq!(eval("(AND true none)"), None);
        assert_eq!(eval("(AND false none)"), None);
        assert_eq!(eval("(AND none none)"), None);
    }

    #[test]
    fn test_runtime_null_propagation() {
        let expression = deserialize("(AND left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Bool), ("right", VarType::Bool)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(false), VariableValue::none()])
                    .as_bool()
            },
            None
        );
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(true), VariableValue::some(false)])
                    .as_bool()
            },
            Some(false)
        );
    }
}
