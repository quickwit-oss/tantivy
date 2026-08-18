//! `CONCAT` joins two or more scalar strings with configurable delimiter behavior.
//!
//! Its production signature is `CONCAT(delimiter, ignore_empty, value1, value2, ...)`, with at
//! least four total arguments. `delimiter` and `ignore_empty` must be string literals. The second
//! literal enables skipping empty values only when it equals `"true"` case-insensitively; every
//! other spelling behaves as false. All remaining arguments are strings.
//!
//! Null propagation across value arguments is strict: any null value makes the result null. When
//! empty strings are not ignored, production inserts a delimiter only after output bytes have
//! already been written. Consequently `CONCAT(",", "false", "", "b")` is `"b"`, while the
//! reversed values produce `"b,"`. Empty output is present, not null.
//!
//! dd-go positionally zips multivalued arguments and returns null on cardinality mismatch. Arrays
//! are outside jitexpr's scalar type model. Constructed bytes live in `CompiledFn`'s fixed-capacity
//! call arena; arena exhaustion returns null.

use std::collections::HashMap;
use std::sync::Arc;

use cranelift::codegen::ir::{
    FuncRef, Function as CraneliftFunction, StackSlotData, StackSlotKind, Type, types,
};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, StringArena, TypedExpr,
    TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_string_concat";
const RAW_INPUT_SIZE: usize = size_of::<RawInput>();

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConcatFnCall {
    arguments: JoinArguments,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct JoinArguments {
    delimiter: Arc<str>,
    ignore_empty: bool,
    values: Box<[TypedExpr]>,
}

impl FnCall for ConcatFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        infer_join_types(Function::Concat, args, target_type, inferred_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        let Some(arguments) = apply_join_types("CONCAT", args, context)? else {
            return Ok(TypedExpr::none());
        };
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(ConcatFnCall { arguments }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        self.arguments.args_mut()
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Str);
        self.arguments.emit_cranelift_ir(context, builder)
    }
}

pub(super) fn infer_join_types<'a>(
    function: Function,
    args: &'a [UntypedExpr],
    target_type: InferredTypeSet,
    inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
) -> Result<InferredTypeSet, TypeError> {
    if target_type.intersect(InferredTypeSet::STRING).is_none() {
        return Err(TypeError::WrongFunctionReturnType {
            function,
            expected: target_type,
            got: InferredTypeSet::STRING,
        });
    }
    if args.len() < 4 {
        return Err(TypeError::InvalidNumberOfArguments {
            function,
            expected: 4,
            got: args.len(),
        });
    }
    for arg in args {
        crate::ast::infer_types_aux(arg, InferredTypeSet::STRING, inferred_types)?;
    }
    Ok(InferredTypeSet::STRING)
}

pub(super) fn apply_join_types(
    function_name: &str,
    args: &[UntypedExpr],
    context: &mut CompileFnBuilder<'_, '_>,
) -> Result<Option<JoinArguments>, CompileError> {
    assert!(
        args.len() >= 4,
        "expected at least 4 args for {function_name}"
    );
    let UntypedExpr::Literal(Literal::String(delimiter)) = &args[0] else {
        panic!("{function_name} delimiter must be a string literal");
    };
    let UntypedExpr::Literal(Literal::String(ignore_empty)) = &args[1] else {
        panic!("{function_name} ignore-empty flag must be a string literal");
    };

    let values = args[2..]
        .iter()
        .map(|arg| context.apply_types(arg, InferredTypeSet::STRING))
        .collect::<Result<Vec<_>, _>>()?;
    if values
        .iter()
        .any(|value| value.return_type == VarType::None)
    {
        return Ok(None);
    }
    debug_assert!(values.iter().all(|value| value.return_type == VarType::Str));

    Ok(Some(JoinArguments {
        delimiter: Arc::from(delimiter.as_ref()),
        ignore_empty: ignore_empty.eq_ignore_ascii_case("true"),
        values: values.into_boxed_slice(),
    }))
}

impl JoinArguments {
    pub(super) fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.values
    }

    pub(super) fn emit_cranelift_ir(
        &self,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let stack_size = self
            .values
            .len()
            .checked_mul(RAW_INPUT_SIZE)
            .and_then(|size| u32::try_from(size).ok())
            .expect("CONCAT argument descriptors exceed Cranelift stack-slot limits");
        let stack_slot = builder.create_sized_stack_slot(StackSlotData::new(
            StackSlotKind::ExplicitSlot,
            stack_size,
            3,
        ));

        for (index, value) in self.values.iter().enumerate() {
            let value = context.compile_expr(value, builder)?;
            let offset = i32::try_from(index * RAW_INPUT_SIZE)
                .expect("CONCAT argument descriptor offset exceeds i32");
            builder
                .ins()
                .stack_store(context.pointer_type(), value.value, stack_slot, offset);
            builder.ins().stack_store(
                context.pointer_type(),
                value.string_len,
                stack_slot,
                offset + 8,
            );
            let is_present = builder.ins().uextend(types::I64, value.is_present);
            builder
                .ins()
                .stack_store(context.pointer_type(), is_present, stack_slot, offset + 16);
        }

        let inputs_ptr = builder
            .ins()
            .stack_addr(context.pointer_type(), stack_slot, 0);
        let input_count = builder.ins().iconst(types::I64, self.values.len() as i64);
        let delimiter_ptr = builder.ins().iconst(
            context.pointer_type(),
            self.delimiter.as_ptr() as usize as i64,
        );
        let delimiter_len = builder
            .ins()
            .iconst(types::I64, self.delimiter.len() as i64);
        let ignore_empty = builder
            .ins()
            .iconst(types::I8, i64::from(self.ignore_empty));
        let string_arena_ptr = context.string_arena_ptr(builder);
        let call = builder.ins().call(
            context.native_functions().string_concat(),
            &[
                inputs_ptr,
                input_count,
                delimiter_ptr,
                delimiter_len,
                ignore_empty,
                string_arena_ptr,
            ],
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
    jit_builder.symbol(SYMBOL, string_concat as *const u8);
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
        AbiParam::new(types::I8),
        AbiParam::new(pointer_type),
    ]);
    signature.returns.push(AbiParam::new(pointer_type));
    signature.returns.push(AbiParam::new(types::I64));
    let function_id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(function_id, function))
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawInput {
    ptr: *const u8,
    len: usize,
    is_present: u64,
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

unsafe extern "C" fn string_concat(
    inputs_ptr: *const RawInput,
    input_count: usize,
    delimiter_ptr: *const u8,
    delimiter_len: usize,
    ignore_empty: u8,
    string_arena: *mut StringArena,
) -> RawStr {
    if inputs_ptr.is_null() || delimiter_ptr.is_null() || string_arena.is_null() {
        return RawStr::none();
    }
    // SAFETY: Generated code constructs exactly `input_count` initialized descriptors in an
    // aligned stack slot that remains live across this call.
    let inputs = unsafe { std::slice::from_raw_parts(inputs_ptr, input_count) };
    // SAFETY: The delimiter is retained by the typed expression and this is its exact byte length.
    let delimiter = unsafe { std::slice::from_raw_parts(delimiter_ptr, delimiter_len) };

    let mut output_len = 0usize;
    for input in inputs {
        if input.is_present == 0 {
            return RawStr::none();
        }
        if ignore_empty != 0 && input.len == 0 {
            continue;
        }
        if output_len > 0 && !delimiter.is_empty() {
            let Some(next_len) = output_len.checked_add(delimiter.len()) else {
                return RawStr::none();
            };
            output_len = next_len;
        }
        let Some(next_len) = output_len.checked_add(input.len) else {
            return RawStr::none();
        };
        output_len = next_len;
    }

    // SAFETY: CompiledFn exclusively owns and passes this arena for the duration of the call.
    let Some(output_ptr) = (unsafe { &mut *string_arena }).allocate(output_len) else {
        return RawStr::none();
    };
    let mut written = 0usize;
    for input in inputs {
        if ignore_empty != 0 && input.len == 0 {
            continue;
        }
        if written > 0 && !delimiter.is_empty() {
            // SAFETY: The exact output length was checked and allocated above.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    delimiter.as_ptr(),
                    output_ptr.add(written),
                    delimiter.len(),
                );
            }
            written += delimiter.len();
        }
        // SAFETY: Present string descriptors contain live UTF-8 pointers of exactly `len` bytes;
        // source allocations precede and do not overlap the new output allocation.
        unsafe {
            std::ptr::copy_nonoverlapping(input.ptr, output_ptr.add(written), input.len);
        }
        written += input.len;
    }
    debug_assert_eq!(written, output_len);
    RawStr {
        ptr: output_ptr,
        len: output_len,
    }
}

impl From<ConcatFnCall> for FnCallEnum {
    fn from(call: ConcatFnCall) -> Self {
        FnCallEnum::Concat(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::{STRING_ARENA_CAPACITY, compile};
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<String> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: These expressions have no inputs and return nullable strings.
        unsafe { compiled.call(&[]).as_str().map(str::to_owned) }
    }

    #[test]
    fn test_requires_four_or_more_string_arguments() {
        let expression = deserialize(r#"(CONCAT "," "false" left right third)"#).unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(inferred_types.get("left"), Some(&InferredTypeSet::STRING));
        assert_eq!(inferred_types.get("right"), Some(&InferredTypeSet::STRING));
        assert_eq!(inferred_types.get("third"), Some(&InferredTypeSet::STRING));

        let expression = deserialize(r#"(CONCAT "," "false" only_one_value)"#).unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Concat,
                expected: 4,
                ..
            })
        ));
    }

    #[test]
    fn test_delimiter_and_ignore_empty_behavior() {
        assert_eq!(
            eval(r#"(CONCAT "," "false" "a" "b" "c")"#).as_deref(),
            Some("a,b,c")
        );
        assert_eq!(
            eval(r#"(CONCAT "," "TrUe" "a" "" "c")"#).as_deref(),
            Some("a,c")
        );
        assert_eq!(
            eval(r#"(CONCAT "," "not-true" "a" "" "c")"#).as_deref(),
            Some("a,,c")
        );
        assert_eq!(eval(r#"(CONCAT "," "false" "" "b")"#).as_deref(), Some("b"));
        assert_eq!(
            eval(r#"(CONCAT "," "false" "b" "")"#).as_deref(),
            Some("b,")
        );
        assert_eq!(eval(r#"(CONCAT "," "true" "" "")"#).as_deref(), Some(""));
    }

    #[test]
    fn test_runtime_null_is_strict() {
        let expression = deserialize(r#"(CONCAT ":" "true" left right)"#).unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();

        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some("a"), VariableValue::none()])
                    .as_str()
            },
            None
        );
    }

    #[test]
    fn test_nested_values_are_stable_and_arena_exhaustion_is_null() {
        let expression = deserialize(r#"(CONCAT ":" "false" (UPPER left) (LOWER right))"#).unwrap();
        let variable_types = HashMap::from([("left", VarType::Str), ("right", VarType::Str)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some("ab"), VariableValue::some("CD")])
                    .as_str()
            },
            Some("AB:cd")
        );

        let oversized = "a".repeat(STRING_ARENA_CAPACITY);
        assert_eq!(
            unsafe {
                compiled
                    .call(&[
                        VariableValue::some(oversized.as_str()),
                        VariableValue::some("b"),
                    ])
                    .as_str()
            },
            None
        );
    }
}
