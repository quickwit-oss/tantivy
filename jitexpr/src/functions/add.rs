// Adds takes an arbitrary number of arguments and adds them.
//
// The type of the addition is rather complex.
// We consider the possible types of all arguments, make an intersection of those, and
// pick the first available type with the order of priority i64, u64, f64.
//
// For instance (ADD mycol 1f64) where mycol is i64 will coerce
// 1f64 to 1i64 at compile time (because we have detected that the conversion was lossless), and the
// operation will run over integer.
//
// On the other hand, (ADD mycol 1.2f64) where mycol is i64 will coerce
// mycol to float dynamically (because 1.2f64 cannot be converted to u64 with loss).
//
// If any of the values of the arguments is NaN, the function return NaN.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AddFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for AddFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::NUMERICAL).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Add,
                expected: target_type,
                got: InferredTypeSet::NUMERICAL,
            });
        }
        let mut return_types = InferredTypeSet::NUMERICAL;
        for arg in args {
            let arg_types =
                crate::ast::infer_types_aux(arg, InferredTypeSet::NUMERICAL, inferred_types)?;
            return_types = return_types.intersect(arg_types);
        }
        return_types = with_float_fallback(return_types);
        let constrained_return_types = return_types.intersect(target_type);
        if constrained_return_types.is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Add,
                expected: target_type,
                got: return_types,
            });
        }
        Ok(constrained_return_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        let mut return_types = InferredTypeSet::NUMERICAL.intersect(target_type_set);
        for arg in args {
            let arg_types = crate::ast::infer_type_with_variable_types(
                arg,
                InferredTypeSet::NUMERICAL,
                context.variable_types(),
            )?;
            return_types = return_types.intersect(arg_types);
        }
        let return_type = select_return_type(with_float_fallback(return_types));
        let typed_args: Vec<TypedExpr> = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::singleton(return_type)))
            .collect::<Result<_, _>>()?;
        if typed_args
            .iter()
            .any(|typed_arg| !is_numerical(typed_arg.return_type))
        {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type,
            ast: TypedExprAst::from_call(AddFnCall {
                args: typed_args.into_boxed_slice(),
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
        let mut sum = match return_type {
            VarType::U64 | VarType::I64 => builder.ins().iconst(types::I64, 0),
            VarType::F64 => builder.ins().f64const(0.0),
            _ => {
                return Err(CompileError::UnsupportedFunctionType {
                    function: Function::Add,
                    return_type,
                });
            }
        };
        let mut is_present = builder.ins().iconst(types::I8, 1);

        for arg in &self.args {
            let lowered = context.compile_expr(arg, builder)?;
            sum = match return_type {
                VarType::U64 | VarType::I64 => builder.ins().iadd(sum, lowered.value),
                VarType::F64 => builder.ins().fadd(sum, lowered.value),
                _ => unreachable!("the return type was checked above"),
            };
            is_present = builder.ins().band(is_present, lowered.is_present);
        }
        Ok(LoweredValue {
            value: sum,
            is_present,
        })
    }
}

fn with_float_fallback(inferred_types: InferredTypeSet) -> InferredTypeSet {
    if inferred_types.is_none() {
        InferredTypeSet::F64
    } else {
        inferred_types
    }
}

fn select_return_type(inferred_types: InferredTypeSet) -> VarType {
    if inferred_types.i64 {
        VarType::I64
    } else if inferred_types.u64 {
        VarType::U64
    } else {
        debug_assert!(inferred_types.f64);
        VarType::F64
    }
}

fn is_numerical(var_type: VarType) -> bool {
    matches!(var_type, VarType::I64 | VarType::U64 | VarType::F64)
}

impl From<AddFnCall> for FnCallEnum {
    fn from(call: AddFnCall) -> Self {
        FnCallEnum::Add(call)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::{Literal, infer_types};
    use crate::compile::{TypedExprAst, compile};
    use crate::types::VariableOpt;

    #[test]
    fn test_infer_types_rejects_string_argument() {
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1.0),
            UntypedExpr::literal("hello"),
        ]);
        let error = infer_types(&expr).unwrap_err();
        assert!(matches!(
            error,
            TypeError::InvalidLiteralType {
                literal: Literal::String(_),
                expected: InferredTypeSet::NUMERICAL,
            }
        ));
    }

    #[test]
    fn test_infer_types_constrains_variables_to_numerical() {
        let expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("a"), UntypedExpr::variable("b")]);
        let inferred_types = infer_types(&expr).unwrap();
        assert_eq!(inferred_types.get("a"), Some(&InferredTypeSet::NUMERICAL));
        assert_eq!(inferred_types.get("b"), Some(&InferredTypeSet::NUMERICAL));
    }

    #[test]
    fn test_call_with_types_preserves_u64() {
        let variable_types = HashMap::from([("present", VarType::U64)]);
        let typed_expr = crate::typed_expr_from_str("(ADD present 1u64)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::U64);
        assert_eq!(
            typed_expr,
            TypedExpr {
                return_type: VarType::U64,
                ast: TypedExprAst::from_call(AddFnCall {
                    args: vec![
                        TypedExprAst::variable("present", VarType::U64).with_type(VarType::U64),
                        TypedExpr::literal(1u64),
                    ]
                    .into_boxed_slice()
                }),
            }
        );
    }

    #[test]
    fn test_call_with_types_rematerializes_compatible_literal_as_u64() {
        let variable_types = HashMap::from([("present", VarType::U64)]);
        let typed_expr = crate::typed_expr_from_str("(ADD present 1i64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::U64);
        assert_eq!(
            typed_expr,
            TypedExpr {
                return_type: VarType::U64,
                ast: TypedExprAst::from_call(AddFnCall {
                    args: vec![
                        TypedExprAst::variable("present", VarType::U64).with_type(VarType::U64),
                        TypedExpr::literal(1u64),
                    ]
                    .into_boxed_slice()
                }),
            }
        );
    }

    #[test]
    fn test_call_with_types_rematerializes_integral_f64_literal_as_u64() {
        let variable_types = HashMap::from([("present", VarType::U64)]);
        let typed_expr = crate::typed_expr_from_str("(ADD 1.0f64 present)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::U64);
        let TypedExprAst::FnCall(FnCallEnum::Add(call)) = typed_expr.ast else {
            panic!("expected an ADD call");
        };
        assert_eq!(call.args[0], TypedExpr::literal(1u64));
    }

    #[test]
    fn test_call_with_types_rematerializes_compatible_literal_as_i64() {
        let variable_types = HashMap::from([("present", VarType::I64)]);
        let typed_expr = crate::typed_expr_from_str("(ADD present 1u64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::I64);
        let TypedExprAst::FnCall(FnCallEnum::Add(call)) = typed_expr.ast else {
            panic!("expected an ADD call");
        };
        assert_eq!(call.args[1], TypedExpr::literal(1i64));
    }

    #[test]
    fn test_call_with_types_prefers_i64_for_compatible_literals() {
        let variable_types = HashMap::new();
        let typed_expr = crate::typed_expr_from_str("(ADD 1u64 2.0f64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::I64);
        let TypedExprAst::FnCall(FnCallEnum::Add(call)) = typed_expr.ast else {
            panic!("expected an ADD call");
        };
        assert_eq!(call.args[0], TypedExpr::literal(1i64));
        assert_eq!(call.args[1], TypedExpr::literal(2i64));
    }

    #[test]
    fn test_call_with_types_uses_u64_when_i64_is_not_possible() {
        let variable_types = HashMap::new();
        let expression = crate::ast::deserialize("(ADD 9223372036854775808u64)").unwrap();
        let typed_expr =
            crate::typed_expr_from_str("(ADD 9223372036854775808u64)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::U64);
        assert_eq!(
            typed_expr,
            TypedExpr {
                return_type: VarType::U64,
                ast: TypedExprAst::from_call(AddFnCall {
                    args: vec![TypedExpr::literal(9223372036854775808u64)].into_boxed_slice(),
                }),
            }
        );

        let compiled = compile(&expression, &variable_types).unwrap();
        let output = unsafe { compiled.call(&[]) };

        assert_eq!(unsafe { output.as_u64() }, Some(9223372036854775808u64));
    }

    #[test]
    fn test_call_with_types_coerces_mixed_numbers_to_f64() {
        let variable_types = HashMap::from([("present", VarType::U64)]);
        let typed_expr = crate::typed_expr_from_str("(ADD present 1.2f64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::F64);
        assert!(matches!(typed_expr.ast, TypedExprAst::FnCall(_)));
    }

    #[test]
    fn test_call_with_types_propagates_missing_variable() {
        let variable_types = HashMap::from([("present", VarType::U64)]);
        let typed_expr =
            crate::typed_expr_from_str("(ADD present (ADD 1u64 missing))", &variable_types);

        assert_eq!(typed_expr, TypedExpr::none());
    }

    #[test]
    fn test_compile_signed_add() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(-4i64),
            UntypedExpr::variable("myfield"),
        ]);
        let variable_types = HashMap::from([("myfield", VarType::I64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(-8i64)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_i64() }, Some(-12));
    }

    #[test]
    fn test_compile_adds_i64_literal_to_u64_variable_without_float_coercion() {
        let variable_types = HashMap::from([("myfield", VarType::U64)]);
        let argument_orders = [
            vec![UntypedExpr::variable("myfield"), UntypedExpr::literal(1i64)],
            vec![UntypedExpr::literal(1i64), UntypedExpr::variable("myfield")],
        ];

        for args in argument_orders {
            let expression = Function::Add.call_untyped_expr(args);
            let compiled = compile(&expression, &variable_types).unwrap();
            let input = [VariableOpt::some(41u64)];
            let output = unsafe { compiled.call(&input) };
            assert_eq!(unsafe { output.as_u64() }, Some(42));
        }
    }

    #[test]
    fn test_compile_coerces_compatible_nested_add_to_u64() {
        let nested_literals = Function::Add
            .call_untyped_expr(vec![UntypedExpr::literal(1i64), UntypedExpr::literal(2u64)]);
        let expression = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("myfield"), nested_literals]);
        let variable_types = HashMap::from([("myfield", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(39u64)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_u64() }, Some(42));
    }

    #[test]
    fn test_compile_coerces_integers_to_float() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("myfield"),
            UntypedExpr::literal(-2i64),
            UntypedExpr::literal(0.5f64),
        ]);
        let variable_types = HashMap::from([("myfield", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(10u64)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_f64() }, Some(8.5));
    }

    #[test]
    fn test_compile_loads_multiple_variable_slots() {
        let expression = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("x"), UntypedExpr::variable("y")]);
        let variable_types = HashMap::from([("x", VarType::U64), ("y", VarType::F64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(10u64), VariableOpt::some(0.5f64)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_f64() }, Some(10.5));
    }

    #[test]
    fn test_compile_add_propagates_none_input() {
        let expression = crate::ast::deserialize(r#"(ADD x 0.5f64)"#).unwrap();
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::none()];
        let output = unsafe { compiled.call(&input) };
        assert_eq!(unsafe { output.as_f64() }, None);
    }

    #[test]
    fn test_compile_u64_to_float_coercion_is_unsigned() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::literal(0.5f64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(u64::MAX)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_f64() }, Some(u64::MAX as f64 + 0.5));
    }

    #[test]
    fn test_compile_reuses_repeated_variable_slot() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::variable("x"),
            UntypedExpr::literal(1u64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(4i64)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(compiled.inputs.len(), 1);
        assert_eq!(compiled.inputs[0].variable_name.as_ref(), "x");
        assert_eq!(compiled.inputs[0].r#type, VarType::U64);
        assert_eq!(compiled.inputs[0].variable_id, 0);
        assert_eq!(compiled.result_type(), VarType::U64);
        assert_eq!(unsafe { output.as_u64() }, Some(9));
    }

    #[test]
    fn test_compile_can_coerce_variable_when_necessary() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::literal(1.2f64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::some(4u64)];
        let output = unsafe { compiled.call(&input) };
        assert_eq!(compiled.inputs.len(), 1);
        assert_eq!(compiled.inputs[0].variable_name.as_ref(), "x");
        assert_eq!(compiled.inputs[0].r#type, VarType::U64);
        assert_eq!(compiled.inputs[0].variable_id, 0);
        assert_eq!(compiled.result_type(), VarType::F64);
        assert_eq!(unsafe { output.as_f64() }, Some(5.2f64));
    }

    #[test]
    fn test_compile_empty_add_uses_zero_identity() {
        let variable_types = HashMap::new();
        let typed_expr = crate::typed_expr_from_str("(ADD)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::I64);

        let expression = Function::Add.call_untyped_expr(Vec::new());
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };
        assert_eq!(unsafe { output.as_i64() }, Some(0));
    }

    #[test]
    fn test_no_variable_works() {
        let args = vec![UntypedExpr::literal(1.2f64), UntypedExpr::literal(1u64)];
        let variable_types = HashMap::new();
        let typed_expr = crate::typed_expr_from_str("(ADD 1.2f64 1u64)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::F64);
        let expression = Function::Add.call_untyped_expr(args);
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };
        assert_eq!(unsafe { output.as_f64() }, Some(2.2f64));
    }
}
