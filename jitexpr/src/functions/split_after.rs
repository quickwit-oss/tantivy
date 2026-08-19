//! `SPLIT_AFTER(input, separator, occurrence)` returns the part of a string after a separator.
//!
//! The separator must be a string constant. The optional occurrence must be a nonnegative integer
//! constant, uses zero-based indexing, and defaults to zero. Matches are literal, case-sensitive,
//! and non-overlapping. A missing occurrence returns a present empty string, while an empty
//! separator returns the input unchanged. Null input, a null constant, or a negative occurrence
//! returns null.

use std::collections::HashMap;
use std::sync::Arc;

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

const SYMBOL: &str = "jitexpr_split_after";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SplitAfterFnCall {
    input: Box<TypedExpr>,
    separator: Arc<str>,
    occurrence: usize,
}

fn constant_occurrence(expression: Option<&UntypedExpr>) -> Option<usize> {
    let Some(expression) = expression else {
        return Some(0);
    };
    let UntypedExpr::Literal(literal) = expression else {
        panic!("SPLIT_AFTER occurrence must be constant");
    };
    match literal {
        Literal::I64(value) => usize::try_from(*value).ok(),
        Literal::U64(value) => usize::try_from(*value).ok(),
        Literal::F64(value) => usize::try_from(*value as i64).ok(),
        Literal::None => None,
        Literal::Bool(_) | Literal::String(_) => {
            unreachable!("type inference constrains SPLIT_AFTER occurrence to an integer")
        }
    }
}

impl FnCall for SplitAfterFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::SplitAfter,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if !(2..=3).contains(&args.len()) {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::SplitAfter,
                expected: 3,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::STRING, inferred_types)?;
        if let Some(occurrence) = args.get(2) {
            crate::ast::infer_types_aux(occurrence, InferredTypeSet::I64, inferred_types)?;
        }
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert!(
            (2..=3).contains(&args.len()),
            "expected 2 or 3 args for SPLIT_AFTER"
        );
        let input = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        let separator = match &args[1] {
            UntypedExpr::Literal(Literal::String(separator)) => Arc::clone(separator),
            UntypedExpr::Literal(Literal::None) => return Ok(TypedExpr::none()),
            _ => panic!("SPLIT_AFTER separator must be a string constant"),
        };
        let Some(occurrence) = constant_occurrence(args.get(2)) else {
            return Ok(TypedExpr::none());
        };
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(SplitAfterFnCall {
                input: Box::new(input),
                separator,
                occurrence,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.input)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "SPLIT_AFTER {} ", self.input)?;
        crate::compile::format_string_literal(&self.separator, formatter)?;
        write!(formatter, " {}u64", self.occurrence)
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
        let separator_ptr = builder
            .ins()
            .iconst(context.pointer_type(), self.separator.as_ptr() as i64);
        let separator_len = builder
            .ins()
            .iconst(types::I64, self.separator.len() as i64);
        let occurrence = builder.ins().iconst(types::I64, self.occurrence as i64);
        let call = builder.ins().call(
            context.native_functions().split_after(),
            &[
                input_ptr,
                input.string_len,
                separator_ptr,
                separator_len,
                occurrence,
            ],
        );
        let value = builder.inst_results(call)[0];
        let string_len = builder.inst_results(call)[1];
        let is_present = builder.ins().icmp_imm_u(IntCC::NotEqual, value, 0);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

pub(super) fn register_jit_symbol(builder: &mut JITBuilder) {
    builder.symbol(SYMBOL, split_after as *const u8);
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
        AbiParam::new(pointer_type),
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

unsafe extern "C" fn split_after(
    input_ptr: *const u8,
    input_len: usize,
    separator_ptr: *const u8,
    separator_len: usize,
    occurrence: usize,
) -> RawStr {
    if input_ptr.is_null() {
        return RawStr::none();
    }
    // SAFETY: CompiledFn supplies a live UTF-8 input and the typed call owns the separator.
    let input =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len)) };
    // SAFETY: The separator pointer and length come from the call's live Arc<str>.
    let separator = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(separator_ptr, separator_len))
    };
    if separator.is_empty() {
        return RawStr::some(input);
    }
    let Some((separator_start, _)) = input.match_indices(separator).nth(occurrence) else {
        return RawStr::some(&input[..0]);
    };
    RawStr::some(&input[separator_start + separator.len()..])
}

impl From<SplitAfterFnCall> for FnCallEnum {
    fn from(call: SplitAfterFnCall) -> Self {
        FnCallEnum::SplitAfter(call)
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
        let mut compiled = compile(&expression, &HashMap::new()).unwrap().context();
        // SAFETY: The expression has no inputs and returns a nullable string.
        unsafe { compiled.call(&[]).as_str().map(str::to_owned) }
    }

    #[test]
    fn test_signature_and_occurrences() {
        for expression in [
            "(SPLIT_AFTER \"a.b\")",
            "(SPLIT_AFTER \"a.b\" \".\" 0i64 1i64)",
        ] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::SplitAfter,
                    expected: 3,
                    ..
                })
            ));
        }

        assert_eq!(eval("(SPLIT_AFTER \"a.b.c\" \".\")"), Some("b.c".into()));
        assert_eq!(
            eval("(SPLIT_AFTER \"a.b.c\" \".\" 0i64)"),
            Some("b.c".into())
        );
        assert_eq!(eval("(SPLIT_AFTER \"a.b.c\" \".\" 1i64)"), Some("c".into()));
    }

    #[test]
    fn test_literal_non_overlapping_and_unicode_matches() {
        assert_eq!(
            eval("(SPLIT_AFTER \"......\" \"...\" 1i64)"),
            Some(String::new())
        );
        assert_eq!(eval("(SPLIT_AFTER \"a...\" \"..\" 0i64)"), Some(".".into()));
        assert_eq!(eval("(SPLIT_AFTER \"α→β→γ\" \"→\" 1i64)"), Some("γ".into()));
    }

    #[test]
    fn test_empty_missing_and_invalid_occurrences() {
        assert_eq!(
            eval("(SPLIT_AFTER \"abc\" \".\" 0i64)"),
            Some(String::new())
        );
        assert_eq!(eval("(SPLIT_AFTER \"abc\" \"\" 3i64)"), Some("abc".into()));
        assert_eq!(eval("(SPLIT_AFTER \"a.b\" \".\" -1i64)"), None);
        assert_eq!(eval("(SPLIT_AFTER \"a.b\" \".\" none)"), None);
        assert_eq!(eval("(SPLIT_AFTER \"a.b\" none)"), None);
    }

    #[test]
    fn test_runtime_null() {
        let expression = deserialize("(SPLIT_AFTER value \".\")").unwrap();
        let mut compiled = compile(&expression, &HashMap::from([("value", VarType::Str)]))
            .unwrap()
            .context();

        // SAFETY: The compiled expression expects one nullable string argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("a.b")]).as_str() },
            Some("b")
        );
        // SAFETY: The compiled expression expects one nullable string argument.
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_str() },
            None
        );
    }
}
