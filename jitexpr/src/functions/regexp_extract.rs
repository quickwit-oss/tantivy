// RegexpExtract extracts a regular-expression match from a string.
//
// It takes three arguments:
// - string: the input string
// - const string: a regular-expression pattern literal. This one CANNOT be the result of another
//   expression
// - const u64: capture index literal. Capture index 0 returns the full match, while indexes 1 and
//   above return
// the corresponding explicit capture group.
//
// It returns None when the input is None, the pattern does not match, or the requested capture
// group is absent or did not participate in the match.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use regex::Regex;

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, RegexRef, TypedExpr,
    TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::{StringRef, VarType};

const SYMBOL: &str = "jitexpr_regexp_extract";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RegexpExtractFnCall {
    regex_ref: RegexRef,
    haystack: Box<TypedExpr>,
    capture_index: u64,
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
        if args.len() != 3 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::RegexpExtract,
                expected: 3,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::STRING, inferred_types)?;
        crate::ast::infer_types_aux(&args[2], InferredTypeSet::NUMERICAL, inferred_types)?;
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 3, "Expected 3 args for regexp_extract");

        let haystack = context.apply_types(&args[0], target_type_set)?;
        if haystack.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        assert_eq!(haystack.return_type, VarType::Str);

        let UntypedExpr::Literal(Literal::String(pattern)) = &args[1] else {
            panic!("regexp_extract pattern must be a string literal");
        };
        let regex = Regex::new(pattern).map_err(|source| CompileError::InvalidRegex {
            pattern: pattern.to_string(),
            source,
        })?;
        let regex_ref = context.register_regex(regex);

        let UntypedExpr::Literal(Literal::U64(capture_index)) = &args[2] else {
            panic!("regexp_extract capture index must be a u64 literal");
        };

        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(RegexpExtractFnCall {
                regex_ref,
                haystack: Box::new(haystack),
                capture_index: *capture_index,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.haystack)
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
        let regex_index = builder
            .ins()
            .iconst(context.pointer_type(), self.regex_ref.index() as i64);
        let capture_index = builder.ins().iconst(types::I64, self.capture_index as i64);
        let match_result = context.regex_match_result(self.regex_ref);
        let match_result = builder
            .ins()
            .iconst(context.pointer_type(), match_result as usize as i64);
        let call = builder.ins().call(
            context.native_functions().regexp_extract(),
            &[
                context.regexes_ptr(),
                regex_index,
                haystack_ptr,
                capture_index,
                match_result,
            ],
        );
        let value = builder.inst_results(call)[0];
        let is_present = builder
            .ins()
            .icmp_imm_u(cranelift::prelude::IntCC::NotEqual, value, 0);
        Ok(LoweredValue { value, is_present })
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
        .extend(std::iter::repeat_n(AbiParam::new(pointer_type), 3));
    signature.params.push(AbiParam::new(types::I64));
    signature.params.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(pointer_type));
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

/// Runtime implementation called by generated code for `RegexpExtract`.
///
/// The JIT only forwards opaque pointers. All knowledge of `Regex` and
/// `StringRef`, including construction of the borrowed result descriptor,
/// stays in this Rust function.
unsafe extern "C" fn regexp_extract(
    regexes: *const Regex,
    regex_index: usize,
    haystack: *const StringRef,
    capture_index: u64,
    match_result: *mut StringRef,
) -> *mut StringRef {
    if haystack.is_null() {
        return std::ptr::null_mut();
    }
    let Ok(capture_index) = usize::try_from(capture_index) else {
        return std::ptr::null_mut();
    };
    // SAFETY: Generated code passes CompiledFn::regexes and an index
    // assigned while constructing that same array.
    let regex = unsafe { &*regexes.add(regex_index) };
    // SAFETY: The contract of CompiledFn::call requires live StringRef
    // input descriptors whose backing bytes contain valid UTF-8.
    let haystack = unsafe { (*haystack).as_str() };
    let Some(regex_match) = regex
        .captures(haystack)
        .and_then(|captures| captures.get(capture_index))
    else {
        return std::ptr::null_mut();
    };

    // SAFETY: Generated code passes a dedicated UnsafeCell-backed result slot.
    // The descriptor borrows bytes from the input rather than copying them.
    unsafe { match_result.write(StringRef::new(regex_match.as_str())) };
    match_result
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
    use crate::types::VariableOpt;

    #[test]
    fn test_infer_types_constrains_haystack_to_string() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)" 0u64)"#).unwrap();

        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(
            inferred_types.get("message"),
            Some(&InferredTypeSet::STRING)
        );
    }

    #[test]
    fn test_compile_returns_borrowed_capture() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 1u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let haystack = "prefix user-123 suffix";
        let mut haystack_ref = StringRef::new(haystack);
        let input = [VariableOpt::some(&mut haystack_ref)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(compiled.regexes.len(), 1);
        assert_eq!(compiled.regexes[0].as_str(), r"([a-z]+)-(\d+)");
        let extracted = unsafe { output.as_str() }.unwrap();
        assert_eq!(extracted, "user");
        assert_eq!(extracted.as_ptr(), haystack[7..].as_ptr());
    }

    #[test]
    fn test_compile_selects_capture_by_index() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 2u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let mut haystack = StringRef::new("user-123");
        let input = [VariableOpt::some(&mut haystack)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("123"));
    }

    #[test]
    fn test_compile_returns_none_without_capture() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)-(\\d+)" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let mut haystack = StringRef::new("no digits here");
        let input = [VariableOpt::some(&mut haystack)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_propagates_none_haystack() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT message "([a-z]+)" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let input = [VariableOpt::none()];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_propagates_compile_time_none_haystack() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT missing "([a-z]+)" 0u64)"#).unwrap();
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };

        assert_eq!(compiled.result_type(), VarType::None);
        assert_eq!(unsafe { output.as_str() }, None);
    }

    #[test]
    fn test_compile_distinguishes_empty_capture_from_none() {
        let expression = ast::deserialize(r#"(REGEXP_EXTRACT "b" "(a*)b" 1u64)"#).unwrap();
        let compiled = compile(&expression, &HashMap::new()).unwrap();
        let output = unsafe { compiled.call(&[]) };

        assert_eq!(unsafe { output.as_str() }, Some(""));
    }

    #[test]
    fn test_compile_group_zero_returns_full_match_without_capture_groups() {
        let expression =
            ast::deserialize(r#"(REGEXP_EXTRACT message "[a-z]+-\\d+" 0u64)"#).unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let mut haystack = StringRef::new("prefix user-123 suffix");
        let input = [VariableOpt::some(&mut haystack)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("user-123"));
    }

    #[test]
    fn test_compile_nested_calls_use_distinct_regexes() {
        let expression = ast::deserialize(
            r#"(REGEXP_EXTRACT
            (REGEXP_EXTRACT message "([a-z]+-\\d+)" 1u64)
            "([a-z]+)"
            1u64)"#,
        )
        .unwrap();
        let variable_types = HashMap::from([("message", VarType::Str)]);
        let compiled = compile(&expression, &variable_types).unwrap();
        let mut haystack = StringRef::new("id=user-123!");
        let input = [VariableOpt::some(&mut haystack)];
        let output = unsafe { compiled.call(&input) };

        assert_eq!(compiled.regexes.len(), 2);
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
