// Adds takes an arbitrary number of arguments and adds them.
//
// The type of the addition is rather complex.
// We consider the possible types of all arguments, make an intersection of those, and
// pick the first available type with the order of priority i64, u64, f64.
//
// For instance (ADD mycol 1f64) where mycol is i64 will actually automatically coerce
// 1f64 to 1i64 (because we have detected that the conversion was lossless), and the operation will
// run over integer.

use std::collections::HashMap;

use cranelift::prelude::{FunctionBuilder, InstBuilder, types};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{CompileError, CompileFnBuilder, LoweringContext, TypedExpr, TypedExprAst};
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
    ) -> Result<cranelift::codegen::ir::Value, CompileError> {
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

        for arg in &self.args {
            let value = context.lower_expr(arg, builder)?;
            sum = match return_type {
                VarType::U64 | VarType::I64 => builder.ins().iadd(sum, value),
                VarType::F64 => builder.ins().fadd(sum, value),
                _ => unreachable!("the return type was checked above"),
            };
        }
        Ok(sum)
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
    use crate::types::VariableValue;

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
        let typed_expr =
            crate::typed_expr_from_str("(ADD 9223372036854775808u64)", &variable_types);

        assert_eq!(typed_expr.return_type, VarType::U64);
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
        let input = [VariableValue { int_i64: -8 }];
        let mut output = VariableValue { int_i64: 0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(unsafe { output.int_i64 }, -12);
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
            let input = [VariableValue { int_u64: 41 }];
            let mut output = VariableValue { int_u64: 0 };

            unsafe { compiled.call(&input, &mut output) };

            assert_eq!(unsafe { output.int_u64 }, 42);
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
        let input = [VariableValue { int_u64: 39 }];
        let mut output = VariableValue { int_u64: 0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(unsafe { output.int_u64 }, 42);
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
        let input = [VariableValue { int_u64: 10 }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, 8.5);
    }

    #[test]
    fn test_compile_loads_multiple_variable_slots() {
        let expression = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("x"), UntypedExpr::variable("y")]);
        let variable_types = HashMap::from([("x", VarType::U64), ("y", VarType::F64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue { int_u64: 10 }, VariableValue { float: 0.5 }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, 10.5);
    }

    #[test]
    fn test_compile_u64_to_float_coercion_is_unsigned() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::literal(0.5f64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue { int_u64: u64::MAX }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, u64::MAX as f64 + 0.5);
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
        let input = [VariableValue { int_u64: 4 }];
        let mut output = VariableValue { int_u64: 0 };

        unsafe { compiled.call(&input, &mut output) };

        assert_eq!(compiled.input_vars.len(), 1);
        assert_eq!(unsafe { output.int_u64 }, 9);
    }

    #[test]
    fn test_compile_can_coerce_variable_when_necessary() {
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::literal(1.2f64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue { int_u64: 4 }];
        let mut output = VariableValue::default();
        unsafe { compiled.call(&input, &mut output) };
        assert_eq!(compiled.input_vars.len(), 1);
        assert_eq!(unsafe { output.float }, 5.2f64);
    }

    #[test]
    fn test_compile_empty_add_uses_zero_identity() {
        let variable_types = HashMap::new();
        let typed_expr = crate::typed_expr_from_str("(ADD)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::I64);

        let expression = Function::Add.call_untyped_expr(Vec::new());
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let mut output = VariableValue { int_i64: 1 };

        unsafe { compiled.call(&[], &mut output) };

        assert_eq!(unsafe { output.int_i64 }, 0);
    }

    #[test]
    fn test_no_variable_works() {
        let args = vec![UntypedExpr::literal(1.2f64), UntypedExpr::literal(1u64)];
        let variable_types = HashMap::new();
        let typed_expr = crate::typed_expr_from_str("(ADD 1.2f64 1u64)", &variable_types);
        assert_eq!(typed_expr.return_type, VarType::F64);
        let expression = Function::Add.call_untyped_expr(args);
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let mut output = VariableValue { int_i64: 1 };
        unsafe { compiled.call(&[], &mut output) };
        assert_eq!(unsafe { output.float }, 2.2f64);
    }
}
