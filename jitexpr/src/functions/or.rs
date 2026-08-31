//! `OR` combines one or more nullable boolean expressions.
//!
//! All arguments must be boolean. Present values are combined with ordinary disjunction. The null
//! rule is intentionally unlike SQL: the result is absent only when every operand is absent.
//! Therefore `TRUE OR NULL = TRUE`, `FALSE OR NULL = FALSE`, and `NULL OR NULL = NULL`.
//!
//! One or more operands are required. The implementation combines their presence bits and ignores
//! the unspecified payload of absent operands.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OrFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for OrFnCall {
    const ARG_COUNT: super::ArgumentCount = super::ArgumentCount::AtLeast(1);

    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Or,
                expected: target_type,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.is_empty() {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Or,
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
        Self::ARG_COUNT.validate(args)?;
        debug_assert!(target_type_set.contains(VarType::Bool));

        let args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::BOOLEAN))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(OrFnCall {
                args: args.into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("OR", self.args.iter(), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        let mut value = builder.ins().iconst(types::I8, 0);
        let mut is_present = value;

        for arg in &self.args {
            let lowered = context.compile_expr(arg, builder)?;
            is_present = builder.ins().bor(is_present, lowered.is_present);
            if arg.return_type != VarType::None {
                let present_value = builder.ins().band(lowered.value, lowered.is_present);
                value = builder.ins().bor(value, present_value);
            }
        }

        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}

impl From<OrFnCall> for FnCallEnum {
    fn from(call: OrFnCall) -> Self {
        FnCallEnum::Or(call)
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
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        // SAFETY: These expressions have no runtime inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_infer_types_requires_boolean_arguments() {
        let expression = deserialize("(OR left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("left"), Some(&InferredTypeSet::BOOLEAN));
        assert_eq!(inferred_types.get("right"), Some(&InferredTypeSet::BOOLEAN));

        let expression = deserialize("(OR)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Or,
                expected: 1,
                got: 0,
            })
        ));
    }

    #[test]
    fn test_present_truth_table_and_variadic_inputs() {
        assert_eq!(eval("(OR false)"), Some(false));
        assert_eq!(eval("(OR false false false)"), Some(false));
        assert_eq!(eval("(OR false true false)"), Some(true));
        assert_eq!(eval("(OR true true)"), Some(true));
    }

    #[test]
    fn test_result_is_absent_only_when_every_argument_is_absent() {
        assert_eq!(eval("(OR true none)"), Some(true));
        assert_eq!(eval("(OR false none)"), Some(false));
        assert_eq!(eval("(OR none none)"), None);
        assert_eq!(eval("(OR none false none)"), Some(false));
    }

    #[test]
    fn test_runtime_null_handling() {
        let expression = deserialize("(OR left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::Bool), ("right", VarType::Bool)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();

        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(false), VariableValue::none()])
                    .as_bool()
            },
            Some(false)
        );
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::none()])
                    .as_bool()
            },
            None
        );
    }
}
