//! `NOT` negates a nullable boolean using Datadog's non-SQL null semantics.
//!
//! It accepts exactly one boolean expression and always returns a present boolean. Present values
//! are inverted normally; an absent input returns `true`. In particular, `NOT(NULL) = TRUE`, not
//! NULL as it would under SQL three-valued logic.
//!
//! This preserves the behavior where a negated predicate also matches documents for which the field
//! is absent. The implementation must therefore select `true` from the child's presence bit instead
//! of reading or negating the unspecified payload of an absent runtime value.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NotFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for NotFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Not,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Not,
                expected: 1,
                got: args.len(),
            });
        }

        crate::ast::infer_types_aux(&args[0], InferredTypeSet::BOOLEAN, inferred_types)?;
        Ok(InferredTypeSet::BOOLEAN)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 1, "expected 1 arg for NOT");
        debug_assert!(target_type_set.contains(VarType::Bool));

        let arg = context.apply_types(&args[0], InferredTypeSet::BOOLEAN)?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(NotFnCall {
                args: vec![arg].into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("NOT", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let arg = context.compile_expr(&self.args[0], builder)?;
        let true_value = builder.ins().iconst(types::I8, 1);
        let value = if self.args[0].return_type == VarType::None {
            true_value
        } else {
            let negated = builder.ins().bxor_imm_u(arg.value, 1);
            builder.ins().select(arg.is_present, negated, true_value)
        };
        Ok(LoweredValue {
            value,
            is_present: true_value,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<NotFnCall> for FnCallEnum {
    fn from(call: NotFnCall) -> Self {
        FnCallEnum::Not(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types, infer_types_with_target};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<bool> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        // SAFETY: These expressions have no runtime inputs and return booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_infer_types_requires_one_boolean_argument() {
        let expression = deserialize("(NOT value)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::BOOLEAN));

        assert!(matches!(
            infer_types_with_target(&expression, InferredTypeSet::STRING),
            Err(TypeError::WrongFunctionReturnType {
                function: Function::Not,
                ..
            })
        ));

        for expression in ["(NOT)", "(NOT true false)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Not,
                    expected: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_negates_present_booleans() {
        assert_eq!(eval("(NOT true)"), Some(false));
        assert_eq!(eval("(NOT false)"), Some(true));
        assert_eq!(eval("(NOT (EQ 1i64 1i64))"), Some(false));
    }

    #[test]
    fn test_absent_input_returns_present_true() {
        assert_eq!(eval("(NOT none)"), Some(true));

        let expression = deserialize("(NOT value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Bool)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_bool() },
            Some(true)
        );
    }
}
