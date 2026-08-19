//! `IF(condition, when_true, when_false)` selects one branch.
//!
//! The condition must be boolean and both branches are coerced to one common bool, string, or
//! numeric type. A null condition returns null. Otherwise only the selected branch's presence bit
//! controls the result; a null in the unselected branch is ignored. Both branch expressions are
//! lowered eagerly.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{InstBuilder, types};

use super::add::with_float_fallback;
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IfFnCall {
    args: Box<[TypedExpr]>,
}

fn common_types(left: InferredTypeSet, right: InferredTypeSet) -> InferredTypeSet {
    let direct = left.intersect(right);
    if direct.is_none()
        && !left.intersect(InferredTypeSet::NUMERICAL).is_none()
        && !right.intersect(InferredTypeSet::NUMERICAL).is_none()
    {
        InferredTypeSet::F64
    } else {
        direct
    }
}
fn select_type(types: InferredTypeSet) -> VarType {
    if types.string {
        VarType::Str
    } else if types.boolean {
        VarType::Bool
    } else {
        super::add::select_return_type(with_float_fallback(
            types.intersect(InferredTypeSet::NUMERICAL),
        ))
    }
}

impl FnCall for IfFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target: InferredTypeSet,
        inferred: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if args.len() != 3 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::If,
                expected: 3,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::BOOLEAN, inferred)?;
        let left = crate::ast::infer_types_aux(&args[1], target, inferred)?;
        let right = crate::ast::infer_types_aux(&args[2], target, inferred)?;
        let result = common_types(left, right).intersect(target);
        if result.is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::If,
                expected: target,
                got: common_types(left, right),
            });
        }
        Ok(result)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 3, "expected 3 args for IF");
        let condition = context.apply_types(&args[0], InferredTypeSet::BOOLEAN)?;
        let left =
            crate::ast::infer_type_with_variable_types(&args[1], target, context.variable_types())?;
        let right =
            crate::ast::infer_type_with_variable_types(&args[2], target, context.variable_types())?;
        let return_type = select_type(common_types(left, right).intersect(target));
        let branch_target = InferredTypeSet::singleton(return_type);
        let when_true = context.apply_types(&args[1], branch_target)?;
        let when_false = context.apply_types(&args[2], branch_target)?;
        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(IfFnCall {
                args: vec![condition, when_true, when_false].into_boxed_slice(),
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("IF", self.args.iter(), formatter)
    }
    fn emit_cranelift_ir(
        &self,
        _return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let condition = context.compile_expr(&self.args[0], builder)?;
        let when_true = context.compile_expr(&self.args[1], builder)?;
        let when_false = context.compile_expr(&self.args[2], builder)?;
        let value = builder
            .ins()
            .select(condition.value, when_true.value, when_false.value);
        let branch_present =
            builder
                .ins()
                .select(condition.value, when_true.is_present, when_false.is_present);
        let is_present = builder.ins().band(condition.is_present, branch_present);
        let string_len = if self.args[1].return_type == VarType::Str {
            builder
                .ins()
                .select(condition.value, when_true.string_len, when_false.string_len)
        } else {
            builder.ins().iconst(types::I64, 0)
        };
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}
impl From<IfFnCall> for FnCallEnum {
    fn from(call: IfFnCall) -> Self {
        FnCallEnum::If(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_signature_and_values() {
        assert!(matches!(
            infer_types(&deserialize("(IF true 1i64)").unwrap()),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::If,
                expected: 3,
                ..
            })
        ));
        for (expr, expected) in [("(IF true 7i64 9i64)", 7), ("(IF false 7i64 9i64)", 9)] {
            let expr = deserialize(expr).unwrap();
            let mut compiled = compile(&expr, &HashMap::new()).unwrap().context();
            assert_eq!(unsafe { compiled.call(&[]).as_i64() }, Some(expected));
        }
        let expr = deserialize("(IF true \"yes\" \"no\")").unwrap();
        let mut compiled = compile(&expr, &HashMap::new()).unwrap().context();
        assert_eq!(unsafe { compiled.call(&[]).as_str() }, Some("yes"));
    }

    #[test]
    fn test_only_selected_null_and_null_condition_propagate() {
        let expr = deserialize("(IF condition yes no)").unwrap();
        let types = HashMap::from([
            ("condition", VarType::Bool),
            ("yes", VarType::I64),
            ("no", VarType::I64),
        ]);
        let mut compiled = compile(&expr, &types).unwrap().context();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[
                        VariableValue::some(true),
                        VariableValue::some(1i64),
                        VariableValue::none(),
                    ])
                    .as_i64()
            },
            Some(1)
        );
        assert_eq!(
            unsafe {
                compiled
                    .call(&[
                        VariableValue::some(false),
                        VariableValue::some(1i64),
                        VariableValue::none(),
                    ])
                    .as_i64()
            },
            None
        );
        assert_eq!(
            unsafe {
                compiled
                    .call(&[
                        VariableValue::none(),
                        VariableValue::some(1i64),
                        VariableValue::some(2i64),
                    ])
                    .as_i64()
            },
            None
        );
    }
}
