//! `UPPER` constructs the Unicode-uppercase form of a string.
//!
//! It accepts exactly one string and returns a newly allocated string without mutating its input.
//! It applies one-to-one Unicode simple case mappings; mappings that would expand one character
//! into several (for example `ß` to `SS`) are not applied. Null input returns null, while an empty
//! input returns a present empty string.
//!
//! Constructed bytes live in `CompiledFn`'s fixed-capacity call arena; arena exhaustion returns
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

const SYMBOL: &str = "jitexpr_string_uppercase";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpperFnCall {
    arg: Box<TypedExpr>,
}

impl FnCall for UpperFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Upper,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if args.len() != 1 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Upper,
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
        assert_eq!(args.len(), 1, "expected 1 arg for UPPER");
        let arg = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        if arg.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        debug_assert_eq!(arg.return_type, VarType::Str);
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(UpperFnCall { arg: Box::new(arg) }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.arg)
    }

    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("UPPER", std::iter::once(self.arg.as_ref()), formatter)
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
        let string_arena_ptr = context.string_arena_ptr(builder);
        let call = builder.ins().call(
            context.native_functions().string_uppercase(),
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
    jit_builder.symbol(SYMBOL, string_uppercase as *const u8);
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

unsafe extern "C" fn string_uppercase(
    input_ptr: *const u8,
    input_len: usize,
    string_arena: *mut StringArena,
) -> RawStr {
    if input_ptr.is_null() || string_arena.is_null() {
        return RawStr::none();
    }

    let output_len = {
        // SAFETY: Generated code passes a live UTF-8 string and its exact byte length for every
        // present string value.
        let input = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len))
        };
        let mut output_len = 0usize;
        for character in input.chars() {
            let uppercase = simple_uppercase(character);
            let Some(next_len) = output_len.checked_add(uppercase.len_utf8()) else {
                return RawStr::none();
            };
            output_len = next_len;
        }
        output_len
    };

    // The input borrow has ended. Nested calls may pass an earlier, disjoint arena allocation.
    // SAFETY: CompiledFn exclusively owns and passes this arena for the duration of the call.
    let Some(output_ptr) = (unsafe { &mut *string_arena }).allocate(output_len) else {
        return RawStr::none();
    };

    // SAFETY: The arena never reallocates and the newly allocated output range does not overlap an
    // earlier arena-backed input range.
    let input =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len)) };
    let mut written = 0usize;
    for character in input.chars() {
        let uppercase = simple_uppercase(character);
        let mut encoded = [0; 4];
        let encoded = uppercase.encode_utf8(&mut encoded).as_bytes();
        // SAFETY: `output_len` was computed from this exact transformation.
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

/// Retain the input when the uppercase mapping expands to preserve one-to-one mappings.
fn simple_uppercase(character: char) -> char {
    let mut uppercase = character.to_uppercase();
    let first = uppercase.next().unwrap_or(character);
    if uppercase.next().is_some() {
        character
    } else {
        first
    }
}

impl From<UpperFnCall> for FnCallEnum {
    fn from(call: UpperFnCall) -> Self {
        FnCallEnum::Upper(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::{STRING_ARENA_CAPACITY, compile};
    use crate::types::VariableValue;

    #[test]
    fn test_requires_one_string_argument() {
        let expression = deserialize("(UPPER value)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("value"), Some(&InferredTypeSet::STRING));

        for expression in ["(UPPER)", "(UPPER one two)"] {
            let expression = deserialize(expression).unwrap();
            assert!(matches!(
                infer_types(&expression),
                Err(TypeError::InvalidNumberOfArguments {
                    function: Function::Upper,
                    expected: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn test_uppercases_unicode_without_full_case_expansion_or_mutation() {
        let expression = deserialize("(UPPER value)").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        let input_string = String::from("café Straße ı");

        let output = unsafe { compiled.call(&[VariableValue::some(input_string.as_str())]) };

        assert_eq!(unsafe { output.as_str() }, Some("CAFÉ STRAßE I"));
        assert_eq!(input_string, "café Straße ı");
    }

    #[test]
    fn test_null_empty_nested_and_arena_exhaustion() {
        let expression = deserialize("(UPPER (UPPER value))").unwrap();
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_str() },
            None
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("")]).as_str() },
            Some("")
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("Hello")]).as_str() },
            Some("HELLO")
        );

        let too_large = "a".repeat(STRING_ARENA_CAPACITY / 2 + 1);
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some(too_large.as_str())])
                    .as_str()
            },
            None
        );
    }
}
