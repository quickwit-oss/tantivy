// RegexpExtract extracts a regular-expression match from a string.
//
// It takes two or three arguments:
// - string: the input string
// - const string: a regular-expression pattern literal. This one CANNOT be the result of another
//   expression
// - optional const u64: capture index literal. It defaults to 0 during conversion to the typed
//   expression. Capture index 0 returns the full match, while indexes 1 and above return the
//   corresponding explicit capture group.
//
// It returns None when the input is None, the pattern does not match, or the requested capture
// group is absent or did not participate in the match.

use std::collections::HashMap;
use std::sync::Arc;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use regex::Regex;

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_regexp_extract";

#[derive(Clone, Debug)]
pub(crate) struct RegexpExtractFnCall {
    regex: Arc<Regex>,
    haystack: Box<TypedExpr>,
    capture_index: u64,
}

impl PartialEq for RegexpExtractFnCall {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
            && self.haystack == other.haystack
            && self.capture_index == other.capture_index
    }
}

impl FnCall for RegexpExtractFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::RegexpExtract,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if !(2..=3).contains(&args.len()) {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::RegexpExtract,
                expected: 3,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::STRING, inferred_types)?;
        if let Some(capture_index) = args.get(2) {
            crate::ast::infer_types_aux(capture_index, InferredTypeSet::NUMERICAL, inferred_types)?;
        }
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert!(
            (2..=3).contains(&args.len()),
            "Expected 2 or 3 args for regexp_extract"
        );

        let haystack = context.apply_types(&args[0], target_type_set)?;
        if haystack.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        assert_eq!(haystack.return_type, VarType::Str);

        let UntypedExpr::Literal(Literal::String(pattern)) = &args[1] else {
            panic!("regexp_extract pattern must be a string literal");
        };
        let regex = Arc::new(
            Regex::new(pattern).map_err(|source| CompileError::InvalidRegex {
                pattern: pattern.to_string(),
                source,
            })?,
        );

        let capture_index = match args.get(2) {
            None => 0,
            Some(UntypedExpr::Literal(Literal::U64(capture_index))) => *capture_index,
            Some(_) => panic!("regexp_extract capture index must be a u64 literal"),
        };

        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(RegexpExtractFnCall {
                regex,
                haystack: Box::new(haystack),
                capture_index,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.haystack)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "REGEXP_EXTRACT {} ", self.haystack)?;
        crate::compile::format_string_literal(self.regex.as_str(), formatter)?;
        write!(formatter, " {}u64", self.capture_index)
    }

    /// Produce CraneLift IR for the given function call.
    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Str);

        let haystack = context.compile_expr(&self.haystack, builder)?;
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let haystack_ptr = builder
            .ins()
            .select(haystack.is_present, haystack.value, null);
        let regex_ptr = builder
            .ins()
            .iconst(context.pointer_type(), Arc::as_ptr(&self.regex) as i64);
        let capture_index = builder.ins().iconst(types::I64, self.capture_index as i64);
        let call = builder.ins().call(
            context.native_functions().regexp_extract(),
            &[regex_ptr, haystack_ptr, haystack.string_len, capture_index],
        );
        let value = builder.inst_results(call)[0];
        let string_len = builder.inst_results(call)[1];
        let is_present = builder
            .ins()
            .icmp_imm_u(cranelift::prelude::IntCC::NotEqual, value, 0);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

pub(super) fn register_jit_symbol(jit_builder: &mut JITBuilder) {
    jit_builder.symbol(SYMBOL, regexp_extract as *const u8);
}

pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer_type: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature
        .params
        .extend(std::iter::repeat_n(AbiParam::new(pointer_type), 2));
    signature.params.push(AbiParam::new(types::I64));
    signature.params.push(AbiParam::new(types::I64));
    signature.returns.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I64));
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

/// Raw two-word string result returned to generated code.
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

/// Runtime implementation called by generated code for `RegexpExtract`.
///
/// The JIT forwards a nullable UTF-8 pointer and byte length. The returned
/// pointer and length borrow directly from the haystack.
unsafe extern "C" fn regexp_extract(
    regex: *const Regex,
    haystack_ptr: *const u8,
    haystack_len: usize,
    capture_index: u64,
) -> RawStr {
    if haystack_ptr.is_null() {
        return RawStr::none();
    }
    let Ok(capture_index) = usize::try_from(capture_index) else {
        return RawStr::none();
    };
    // SAFETY: Generated code embeds a pointer to the Arc-owned Regex stored in
    // the typed expression retained by CompiledFn.
    let regex = unsafe { &*regex };
    // SAFETY: The contract of CompiledFn::call requires a live UTF-8 string
    // pointer and its exact byte length for every present string input.
    let haystack = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(haystack_ptr, haystack_len))
    };
    let Some(regex_match) = regex
        .captures(haystack)
        .and_then(|captures| captures.get(capture_index))
    else {
        return RawStr::none();
    };

    RawStr::some(regex_match.as_str())
}

impl From<RegexpExtractFnCall> for FnCallEnum {
    fn from(call: RegexpExtractFnCall) -> Self {
        FnCallEnum::RegexpExtract(call)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::{self, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    #[test]
    fn test_infer_types_constrains_haystack_to_string() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)")"#).unwrap();

        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(
            inferred_types.get("message"),
            Some(&InferredTypeSet::STRING)
        );
    }

    #[test]
    fn test_infer_types_accepts_optional_capture_index() {
        for expression in [
            r#"(REGEXP_EXTRACT message "([a-z]+)")"#,
            r#"(REGEXP_EXTRACT message "([a-z]+)" 1u64)"#,
        ] {
            let expression = ast::deserialize(expression).unwrap();
            assert!(infer_types(&expression).is_ok());
        }

        for expression in [
            r#"(REGEXP_EXTRACT message)"#,
            r#"(REGEXP_EXTRACT message "([a-z]+)" 0u64 1u64)"#,
        ] {
            let expression = ast::deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::RegexpExtract,
                    expected: 3,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_compile_returns_borrowed_capture() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 1u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let haystack = "prefix user-123 suffix";
        let input = [VariableValue::some(haystack)];
        let output = unsafe { compiled.call(&input) };

        let extracted = unsafe { output.as_str() }.unwrap();
        assert_eq!(extracted, "user");
        assert_eq!(extracted.as_ptr(), haystack[7..].as_ptr());
    }

    #[test]
    fn test_compile_selects_capture_by_index() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 2u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::some("user-123")];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("123"));
    }

    #[test]
    fn test_compile_returns_none_without_capture() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::some("no digits here")];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_propagates_none_haystack() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::none()];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_propagates_compile_time_none_haystack() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT missing "([a-z]+)" 0u64)"#).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        assert_eq!(compiled.result_type(), VarType::None);
        let output = unsafe { compiled.call(&[]) };

        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_distinguishes_empty_capture_from_none() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT "b" "(a*)b" 1u64)"#).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };

        assert_eq!(unsafe { output.as_str() }, Some(""));
    }

    #[test]
    fn test_compile_omitted_group_defaults_to_full_match() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT message "[a-z]+-\\d+")"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::some("prefix user-123 suffix")];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("user-123"));
    }

    #[test]
    fn test_compile_group_zero_returns_full_match_without_capture_groups() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "[a-z]+-\\d+" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::some("prefix user-123 suffix")];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("user-123"));
    }

    #[test]
    fn test_compile_nested_calls_use_their_own_regexes() {
        let expression = ast::deserialize(
            r#"(REGEXP_EXTRACT
            (REGEXP_EXTRACT message "([a-z]+-\\d+)" 1u64)
            "([a-z]+)"
            1u64)"#,
        )
        .unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableValue::some("id=user-123!")];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("user"));
    }

    #[test]
    fn test_compile_rejects_invalid_pattern() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT "anything" "(" 0u64)"#).unwrap();
        let error = compile(&expression, &HashMap::new()).err().unwrap();
        assert!(matches!(
            error,
            CompileError::InvalidRegex { pattern, .. } if pattern == "("
        ));
    }
}
