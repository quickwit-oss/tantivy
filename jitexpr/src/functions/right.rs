//! `RIGHT(input, length)` returns the last `length` UTF-8 bytes of a string.
//!
//! The length must be an integer constant and is stored as a `usize`. A length greater than the
//! input byte length returns the whole string, zero returns a present empty string, and null input
//! propagates. Negative lengths and lengths that place the start inside a UTF-8 code point return
//! null.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{InstBuilder, IntCC};

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RightFnCall {
    input: Box<TypedExpr>,
    length: usize,
}

fn constant_length(expression: &UntypedExpr) -> Option<usize> {
    let UntypedExpr::Literal(literal) = expression else {
        panic!("RIGHT length must be constant");
    };
    match literal {
        Literal::I64(value) => usize::try_from(*value).ok(),
        Literal::U64(value) => usize::try_from(*value).ok(),
        Literal::F64(value) => usize::try_from(*value as i64).ok(),
        Literal::None => None,
        Literal::Bool(_) | Literal::String(_) => {
            unreachable!("type inference constrains RIGHT length to an integer")
        }
    }
}

impl FnCall for RightFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Right,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Right,
                expected: 2,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::I64, inferred_types)?;
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for RIGHT");
        let input = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        let Some(length) = constant_length(&args[1]) else {
            return Ok(TypedExpr::none());
        };
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(RightFnCall {
                input: Box::new(input),
                length,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.input)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Str);
        let input = context.compile_expr(&self.input, builder)?;
        let zero = builder.ins().iconst(context.pointer_type(), 0);
        let input_ptr = builder.ins().select(input.is_present, input.value, zero);
        let length = builder
            .ins()
            .iconst(context.pointer_type(), self.length as i64);
        let length_is_shorter =
            builder
                .ins()
                .icmp(IntCC::UnsignedLessThan, length, input.string_len);
        let suffix_start = builder.ins().isub(input.string_len, length);
        let start = builder.ins().select(length_is_shorter, suffix_start, zero);
        let call = builder.ins().call(
            context.native_functions().substring(),
            &[input_ptr, input.string_len, start, length],
        );
        let value = builder.inst_results(call)[0];
        let string_len = builder.inst_results(call)[1];
        let native_succeeded = builder.ins().icmp_imm_u(IntCC::NotEqual, value, 0);
        let is_present = builder.ins().band(input.is_present, native_succeeded);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

impl From<RightFnCall> for FnCallEnum {
    fn from(call: RightFnCall) -> Self {
        FnCallEnum::Right(call)
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
        // SAFETY: The expression has no inputs and returns a nullable string.
        unsafe { compiled.call(&[]).as_str().map(str::to_owned) }
    }

    #[test]
    fn test_signature_and_byte_lengths() {
        for expression in ["(RIGHT \"abc\")", "(RIGHT \"abc\" 1i64 2i64)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Right,
                    expected: 2,
                    ..
                })
            ));
        }

        assert_eq!(eval("(RIGHT \"abcdef\" 3i64)"), Some("def".into()));
        assert_eq!(eval("(RIGHT \"éclair\" 5i64)"), Some("clair".into()));
        assert_eq!(eval("(RIGHT \"éclair\" 6i64)"), None);
    }

    #[test]
    fn test_zero_clamping_and_invalid_length() {
        assert_eq!(eval("(RIGHT \"abc\" 0i64)"), Some(String::new()));
        assert_eq!(eval("(RIGHT \"abc\" 99i64)"), Some("abc".into()));
        assert_eq!(eval("(RIGHT \"abc\" -1i64)"), None);
    }

    #[test]
    fn test_runtime_null() {
        let expression = deserialize("(RIGHT value 2i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::Str)])).unwrap();
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("abc")]).as_str() },
            Some("bc")
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_str() },
            None
        );
    }

    #[test]
    fn test_utf8_boundary_break_returns_null() {
        let expression = deserialize(r#"(RIGHT "下北沢" 1i64)"#).unwrap();
        let mut compiled = compile(&expression, &HashMap::default()).unwrap();
        assert_eq!(unsafe { compiled.call(&[]).as_str() }, None);
        let expression = deserialize(r#"(RIGHT "下北沢" 3i64)"#).unwrap();
        let mut compiled = compile(&expression, &HashMap::default()).unwrap();
        assert_eq!(unsafe { compiled.call(&[]).as_str() }, Some("沢"));
    }
}
