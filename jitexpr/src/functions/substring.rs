//! `SUBSTRING(input, start, length)` returns a byte-indexed string slice.
//!
//! The start and length arguments must be integer constants. They count UTF-8 bytes, not Unicode
//! scalar values or grapheme clusters. For example, `SUBSTRING("éclair", 2, 5)` returns `"clair"`
//! because `é` occupies bytes 0 and 1. The end is clamped to the input length, an out-of-range
//! start or zero length returns an empty string, and null input propagates.
//!
//! Negative bounds and ranges that split a UTF-8 code point return null.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder, IntCC};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_substring";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SubstringFnCall {
    input: Box<TypedExpr>,
    start: usize,
    length: usize,
}

fn constant_usize(expression: &UntypedExpr) -> Option<usize> {
    let UntypedExpr::Literal(literal) = expression else {
        panic!("SUBSTRING bounds must be constants");
    };
    match literal {
        Literal::I64(value) => usize::try_from(*value).ok(),
        Literal::U64(value) => i64::try_from(*value)
            .ok()
            .and_then(|value| usize::try_from(value).ok()),
        Literal::F64(value) if literal.types().contains(VarType::I64) => {
            usize::try_from(*value as i64).ok()
        }
        Literal::None => None,
        Literal::Bool(_) | Literal::F64(_) | Literal::String(_) => {
            panic!("SUBSTRING bounds must be integer constants")
        }
    }
}

impl FnCall for SubstringFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Substring,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if args.len() != 3 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Substring,
                expected: 3,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::I64, inferred_types)?;
        crate::ast::infer_types_aux(&args[2], InferredTypeSet::I64, inferred_types)?;
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 3, "expected 3 args for SUBSTRING");
        let input = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        let start = constant_usize(&args[1]);
        let length = constant_usize(&args[2]);
        let (Some(start), Some(length)) = (start, length) else {
            return Ok(TypedExpr::none());
        };
        if input.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }

        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(SubstringFnCall {
                input: Box::new(input),
                start,
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
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let input_ptr = builder.ins().select(input.is_present, input.value, null);
        let start = builder
            .ins()
            .iconst(context.pointer_type(), self.start as i64);
        let length = builder
            .ins()
            .iconst(context.pointer_type(), self.length as i64);
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

pub(super) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(SYMBOL, substring as *const u8);
}

pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature.params.extend([
        AbiParam::new(pointer_type),
        AbiParam::new(types::I64),
        AbiParam::new(types::I64),
        AbiParam::new(types::I64),
    ]);
    signature
        .returns
        .extend([AbiParam::new(pointer_type), AbiParam::new(types::I64)]);
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

#[repr(C)]
struct RawStr {
    ptr: *const u8,
    len: usize,
}

impl RawStr {
    fn none() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    fn some(value: &str) -> Self {
        Self {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }
}

unsafe extern "C" fn substring(
    input_ptr: *const u8,
    input_len: usize,
    start: usize,
    length: usize,
) -> RawStr {
    if input_ptr.is_null() {
        return RawStr::none();
    }
    // SAFETY: CompiledFn's call contract guarantees a live UTF-8 string pointer and exact length.
    let input =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len)) };
    let end = start.saturating_add(length).min(input.len());
    if start >= end {
        return RawStr::some(&input[..0]);
    }
    if !input.is_char_boundary(start) || !input.is_char_boundary(end) {
        return RawStr::none();
    }
    RawStr::some(&input[start..end])
}

impl From<SubstringFnCall> for FnCallEnum {
    fn from(call: SubstringFnCall) -> Self {
        FnCallEnum::Substring(call)
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
    fn test_signature_and_byte_offsets() {
        for expression in [
            "(SUBSTRING \"abc\" 1i64)",
            "(SUBSTRING \"abc\" 1i64 2i64 3i64)",
        ] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Substring,
                    expected: 3,
                    ..
                })
            ));
        }
        assert_eq!(eval("(SUBSTRING \"abcdef\" 1i64 3i64)"), Some("bcd".into()));
        assert_eq!(
            eval("(SUBSTRING \"éclair\" 2i64 5i64)"),
            Some("clair".into())
        );
    }

    #[test]
    fn test_empty_and_clamping() {
        assert_eq!(eval("(SUBSTRING \"abc\" 1i64 99i64)"), Some("bc".into()));
        assert_eq!(eval("(SUBSTRING \"abc\" 3i64 1i64)"), Some(String::new()));
        assert_eq!(eval("(SUBSTRING \"abc\" 9i64 1i64)"), Some(String::new()));
        assert_eq!(eval("(SUBSTRING \"abcdef\" 2i64 2i64)"), Some("cd".into()));
        assert_eq!(eval("(SUBSTRING \"abc\" 1i64 0i64)"), Some(String::new()));
    }

    #[test]
    fn test_invalid_bounds_and_runtime_null() {
        assert_eq!(eval("(SUBSTRING \"é\" 1i64 2i64)"), None);
        assert_eq!(eval("(SUBSTRING \"abc\" -1i64 1i64)"), None);

        let expression = deserialize("(SUBSTRING value 0i64 2i64)").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::Str)])).unwrap();
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("abc")]).as_str() },
            Some("ab")
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_str() },
            None
        );
    }
}
