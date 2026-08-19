//! `LOWER` constructs the Unicode-lowercase form of a string.
//!
//! It accepts exactly one string and returns a newly allocated string without mutating its input.
//! It applies one-to-one Unicode simple case mappings. In particular, `İ` maps to plain `i` rather
//! than expanding to `i` followed by a combining dot. Null input returns null, while empty input
//! remains present.
//!
//! Constructed bytes live in the caller's fixed-capacity string arena; arena exhaustion returns
//! null.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, StringArena, TypedExpr,
    TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_string_lowercase";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LowerFnCall {
    arg: Box<TypedExpr>,
}

impl FnCall for LowerFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Lower,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Lower,
                expected: 1,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::STRING, inferred_types)?;
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 1, "Expected 1 arg for LOWER");
        let arg = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        debug_assert_eq!(arg.return_type, VarType::Str);
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(LowerFnCall { arg: Box::new(arg) }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("LOWER", std::iter::once(self.arg.as_ref()), formatter)
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Str);
        let arg = context.compile_expr(&self.arg, builder)?;
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let input_ptr = builder.ins().select(arg.is_present, arg.value, null);
        let string_lowercase = context.native_functions().string_lowercase();
        let string_arena_ptr = context.string_arena_ptr(builder);
        let call = builder.ins().call(
            string_lowercase,
            &[input_ptr, arg.string_len, string_arena_ptr],
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
    jit_builder.symbol(SYMBOL, string_lowercase as *const u8);
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
    ]);
    signature.returns.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I64));
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
}

unsafe extern "C" fn string_lowercase(
    input_ptr: *const u8,
    input_len: usize,
    string_arena: *mut StringArena,
) -> RawStr {
    if input_ptr.is_null() || string_arena.is_null() {
        return RawStr::none();
    }

    let output_len = {
        // SAFETY: Generated code passes a live UTF-8 string and its exact byte
        // length for every present string value.
        let input = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len))
        };
        let mut output_len = 0usize;
        for character in input.chars() {
            let lowercase = simple_lowercase(character);
            let Some(next_len) = output_len.checked_add(lowercase.len_utf8()) else {
                return RawStr::none();
            };
            output_len = next_len;
        }
        output_len
    };

    // The input reference above is no longer live. This matters for nested
    // LOWER calls whose input points into an earlier, disjoint arena allocation.
    // SAFETY: The caller exclusively borrows and passes this arena for the call.
    let Some(output_ptr) = (unsafe { &mut *string_arena }).allocate(output_len) else {
        return RawStr::none();
    };

    // SAFETY: Same input contract as above. The fixed arena never reallocates,
    // and its new output range starts after any arena-backed input range.
    let input =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len)) };
    let mut written = 0usize;
    for character in input.chars() {
        let lowercase = simple_lowercase(character);
        let mut encoded = [0; 4];
        let encoded = lowercase.encode_utf8(&mut encoded).as_bytes();
        // SAFETY: output_len was computed from this exact transformation,
        // and StringArena reserved that many bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(encoded.as_ptr(), output_ptr.add(written), encoded.len());
        }
        written += encoded.len();
    }
    debug_assert_eq!(written, output_len);
    RawStr {
        ptr: output_ptr,
        len: output_len,
    }
}

/// Use the first code point from a full lowercase mapping to keep the result one-to-one.
fn simple_lowercase(character: char) -> char {
    character.to_lowercase().next().unwrap_or(character)
}

impl From<LowerFnCall> for FnCallEnum {
    fn from(call: LowerFnCall) -> Self {
        FnCallEnum::Lower(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::{STRING_ARENA_CAPACITY, compile};
    use crate::types::VariableValue;

    #[test]
    fn test_infer_types_constrains_argument_to_string() {
        let expression = deserialize("(LOWER value)").unwrap();

        let inferred_types = infer_types(&expression).unwrap();

        assert_eq!(expression.to_string(), "(LOWER value)");
        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::STRING));
    }

    #[test]
    fn test_infer_types_requires_one_argument() {
        for expression in ["(LOWER)", "(LOWER one two)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Lower,
                    expected: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_compile_lowercases_unicode_without_mutating_input() {
        let expression = deserialize("(LOWER value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        let input_string = String::from("CAFÉ İSTANBUL");
        let input = [VariableValue::some(input_string.as_str())];

        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, Some("café istanbul"));
        assert_eq!(input_string, "CAFÉ İSTANBUL");
    }

    #[test]
    fn test_compile_propagates_none_and_preserves_empty_string() {
        let expression = deserialize("(LOWER value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();

        let none = unsafe { compiled.call(&[VariableValue::none()]) };
        assert_eq!(unsafe { none.as_str() }, None);

        let empty = unsafe { compiled.call(&[VariableValue::some("")]) };
        assert_eq!(unsafe { empty.as_str() }, Some(""));
    }

    #[test]
    fn test_nested_lower_uses_stable_arena_allocations() {
        let expression = deserialize("(LOWER (LOWER value))").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        let input = [VariableValue::some("HeLLo")];

        assert_eq!(unsafe { compiled.call(&input).as_str() }, Some("hello"));
    }

    #[test]
    fn test_multiple_lower_calls_keep_previous_allocations_valid() {
        let expression = deserialize("(EQ (LOWER left) (LOWER right))").unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        let input = [VariableValue::some("FiRsT"), VariableValue::some("SeCoNd")];

        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_bool() }, Some(false));
        assert_eq!(compiled.string_arena.used_bytes(), 11);
    }

    #[test]
    fn test_arena_exhaustion_returns_none_without_advancing_cursor() {
        let expression = deserialize("(LOWER value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        let too_large = "A".repeat(STRING_ARENA_CAPACITY + 1);
        let input = [VariableValue::some(too_large.as_str())];

        let output = unsafe { compiled.call(&input) };

        assert_eq!(unsafe { output.as_str() }, None);
        assert_eq!(compiled.string_arena.used_bytes(), 0);
    }

    #[test]
    fn test_arena_cursor_is_cleared_before_each_call() {
        let expression = deserialize("(LOWER value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap().context();
        let full = "A".repeat(STRING_ARENA_CAPACITY);

        {
            let output = unsafe { compiled.call(&[VariableValue::some(full.as_str())]) };
            assert_eq!(
                unsafe { output.as_str() }.unwrap().len(),
                STRING_ARENA_CAPACITY
            );
        }
        assert_eq!(compiled.string_arena.used_bytes(), STRING_ARENA_CAPACITY);

        {
            let output = unsafe { compiled.call(&[VariableValue::some("ABC")]) };
            assert_eq!(unsafe { output.as_str() }, Some("abc"));
        }
        assert_eq!(compiled.string_arena.used_bytes(), 3);
    }
}
