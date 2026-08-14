use std::collections::HashMap;
use std::mem::{self, size_of};

use cranelift::codegen::ir::{MemFlagsData, UserFuncName};
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Module, default_libcall_names};

use crate::ast::{
    Function, Literal, TypedExpr, TypedExprAst, TypedVariable, UntypedExpr, apply_types,
};
use crate::types::{StringRef, VarType, VariableValue};

/// An expression compiled to native machine code.
///
/// This object owns the JIT module containing its executable memory.
pub struct CompiledFunction {
    pub(crate) entry: JitEntry,
    pub(crate) _module: JITModule,
    // String literals are addressed directly by the generated code. Keeping
    // their descriptors here gives those addresses the lifetime of the JIT.
    pub(crate) _literal_strings: Box<[StringRef]>,
    pub input_vars: Vec<TypedVariable>,
    pub typed_expr: TypedExpr,
}

impl CompiledFunction {
    /// Evaluate the compiled expression.
    ///
    /// # Safety
    ///
    /// `args` must follow `input_vars` exactly: every slot must contain the
    /// union member corresponding to that variable's type. `result` must be a
    /// valid writable slot and any referenced strings must remain alive for
    /// the duration of this call.
    pub unsafe fn call(&self, args: &[VariableValue], result: &mut VariableValue) {
        debug_assert_eq!(args.len(), self.input_vars.len());
        // SAFETY: Guaranteed by the caller.
        unsafe { (self.entry)(args.as_ptr(), result) };
    }
}

type JitEntry = unsafe extern "C" fn(*const VariableValue, *mut VariableValue);

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("JIT compilation failed: {0}")]
    Module(#[source] Box<cranelift_module::ModuleError>),
    #[error("input variable {variable_id} has an address offset that is too large")]
    InputOffsetOverflow { variable_id: usize },
    #[error("cannot coerce an expression from {from_type:?} to {target:?}")]
    UnsupportedCoercion { from_type: VarType, target: VarType },
    #[error("cannot compile {function:?} with result type {return_type:?}")]
    UnsupportedFunctionType {
        function: Function,
        return_type: VarType,
    },
}

impl From<cranelift_module::ModuleError> for CompileError {
    fn from(error: cranelift_module::ModuleError) -> Self {
        CompileError::Module(Box::new(error))
    }
}

pub fn compile(
    untyped_expr: &UntypedExpr,
    var_types: &HashMap<&str, VarType>,
) -> Result<CompiledFunction, CompileError> {
    let (typed_expr, input_vars) = apply_types(untyped_expr, var_types);
    compile_typed_expr(typed_expr, input_vars)
}

fn compile_typed_expr(
    expression: TypedExpr,
    input_vars: Vec<TypedVariable>,
) -> Result<CompiledFunction, CompileError> {
    let jit_builder = JITBuilder::new(default_libcall_names())?;
    let mut module = JITModule::new(jit_builder);
    let target_config = module.target_config();
    let pointer_type = target_config.pointer_type();

    // The native entry point mirrors JitEntry: both arguments are pointers and
    // the expression result is written into the second one.
    let mut signature = module.make_signature();
    signature.params.push(AbiParam::new(pointer_type));
    signature.params.push(AbiParam::new(pointer_type));
    let function_id = module.declare_anonymous_function(&signature)?;

    let mut context = module.make_context();
    context.func.signature = signature;
    context.func.name = UserFuncName::user(0, function_id.as_u32());

    let mut function_builder_context = FunctionBuilderContext::new();
    let literal_strings = collect_literal_strings(&expression).into_boxed_slice();
    let mut next_literal_string = 0;
    {
        let mut builder = FunctionBuilder::new(&mut context.func, &mut function_builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let args_ptr = builder.block_params(entry_block)[0];
        let result_ptr = builder.block_params(entry_block)[1];
        let value = lower_expr(
            &expression,
            args_ptr,
            pointer_type,
            &literal_strings,
            &mut next_literal_string,
            &mut builder,
        )?;
        debug_assert_eq!(next_literal_string, literal_strings.len());
        builder
            .ins()
            .store(MemFlagsData::trusted(), value, result_ptr, 0);
        builder.ins().return_(&[]);
        builder.finalize(target_config);
    }

    module.define_function(function_id, &mut context)?;
    module.finalize_definitions()?;

    let code = module.get_finalized_function(function_id);
    // SAFETY: `code` is the finalized entry point for the function whose ABI
    // was built above to exactly match `JitEntry`. The module is retained by
    // `CompiledFunction`, so its executable allocation outlives `entry`.
    let entry = unsafe { mem::transmute::<*const u8, JitEntry>(code) };

    Ok(CompiledFunction {
        entry,
        _module: module,
        _literal_strings: literal_strings,
        input_vars,
        typed_expr: expression,
    })
}

fn lower_expr(
    expression: &TypedExpr,
    args_ptr: Value,
    pointer_type: Type,
    literal_strings: &[StringRef],
    next_literal_string: &mut usize,
    builder: &mut FunctionBuilder<'_>,
) -> Result<Value, CompileError> {
    match &expression.ast {
        TypedExprAst::Literal(literal) => Ok(lower_literal(
            literal,
            pointer_type,
            literal_strings,
            next_literal_string,
            builder,
        )),
        TypedExprAst::Variable(variable) => {
            let byte_offset = variable
                .variable_id
                .checked_mul(size_of::<VariableValue>())
                .and_then(|offset| i32::try_from(offset).ok())
                .ok_or(CompileError::InputOffsetOverflow {
                    variable_id: variable.variable_id,
                })?;
            Ok(builder.ins().load(
                cranelift_type(variable.r#type, pointer_type),
                MemFlagsData::trusted(),
                args_ptr,
                byte_offset,
            ))
        }
        TypedExprAst::Coerce { target_type, expr } => {
            let source_type = expr.return_type;
            let value = lower_expr(
                expr,
                args_ptr,
                pointer_type,
                literal_strings,
                next_literal_string,
                builder,
            )?;
            lower_coercion(value, source_type, *target_type, builder)
        }
        TypedExprAst::Call { function, args } => match function {
            Function::Add => lower_add(
                args,
                expression.return_type,
                args_ptr,
                pointer_type,
                literal_strings,
                next_literal_string,
                builder,
            ),
        },
    }
}

fn lower_literal(
    literal: &Literal,
    pointer_type: Type,
    literal_strings: &[StringRef],
    next_literal_string: &mut usize,
    builder: &mut FunctionBuilder<'_>,
) -> Value {
    match literal {
        Literal::None => builder.ins().iconst(types::I64, 0),
        Literal::Bool(value) => builder.ins().iconst(types::I8, i64::from(*value)),
        Literal::U64(value) => builder.ins().iconst(types::I64, *value as i64),
        Literal::I64(value) => builder.ins().iconst(types::I64, *value),
        Literal::F64(value) => builder.ins().f64const(Ieee64::with_bits(value.to_bits())),
        Literal::String(_) => {
            let string_ref = &literal_strings[*next_literal_string];
            *next_literal_string += 1;
            let string_ref_ptr = (string_ref as *const StringRef) as usize;
            builder.ins().iconst(pointer_type, string_ref_ptr as i64)
        }
    }
}

fn lower_coercion(
    value: Value,
    source: VarType,
    target: VarType,
    builder: &mut FunctionBuilder<'_>,
) -> Result<Value, CompileError> {
    let coerced = match (source, target) {
        (source, target) if source == target => value,
        (VarType::U64, VarType::F64) => builder.ins().fcvt_from_uint(types::F64, value),
        (VarType::I64, VarType::F64) => builder.ins().fcvt_from_sint(types::F64, value),
        // Cranelift integers do not carry signedness. These two coercions have
        // the same machine representation and therefore need no instruction.
        (VarType::U64, VarType::I64) | (VarType::I64, VarType::U64) => value,
        _ => {
            return Err(CompileError::UnsupportedCoercion {
                from_type: source,
                target,
            });
        }
    };
    Ok(coerced)
}

fn lower_add(
    args: &[TypedExpr],
    return_type: VarType,
    args_ptr: Value,
    pointer_type: Type,
    literal_strings: &[StringRef],
    next_literal_string: &mut usize,
    builder: &mut FunctionBuilder<'_>,
) -> Result<Value, CompileError> {
    let mut sum = match return_type {
        VarType::U64 | VarType::I64 => builder.ins().iconst(types::I64, 0),
        VarType::F64 => builder.ins().f64const(Ieee64::with_bits(0)),
        _ => {
            return Err(CompileError::UnsupportedFunctionType {
                function: Function::Add,
                return_type,
            });
        }
    };

    for arg in args {
        let value = lower_expr(
            arg,
            args_ptr,
            pointer_type,
            literal_strings,
            next_literal_string,
            builder,
        )?;
        sum = match return_type {
            VarType::U64 | VarType::I64 => builder.ins().iadd(sum, value),
            VarType::F64 => builder.ins().fadd(sum, value),
            _ => unreachable!("the return type was checked above"),
        };
    }
    Ok(sum)
}

fn collect_literal_strings(expression: &TypedExpr) -> Vec<StringRef> {
    let mut literal_strings = Vec::new();
    collect_literal_strings_aux(expression, &mut literal_strings);
    literal_strings
}

fn collect_literal_strings_aux(expression: &TypedExpr, literal_strings: &mut Vec<StringRef>) {
    match &expression.ast {
        TypedExprAst::Literal(Literal::String(value)) => {
            literal_strings.push(StringRef::new(value));
        }
        TypedExprAst::Literal(_) | TypedExprAst::Variable(_) => {}
        TypedExprAst::Coerce { expr, .. } => collect_literal_strings_aux(expr, literal_strings),
        TypedExprAst::Call { args, .. } => {
            for arg in args {
                collect_literal_strings_aux(arg, literal_strings);
            }
        }
    }
}

fn cranelift_type(var_type: VarType, pointer_type: Type) -> Type {
    match var_type {
        VarType::Bool => types::I8,
        VarType::F64 => types::F64,
        VarType::U64 | VarType::I64 | VarType::None => types::I64,
        VarType::Str => pointer_type,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::{Function, UntypedExpr};
    use crate::types::VarType;

    #[test]
    fn test_compile_signed_add() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(-4i64),
            UntypedExpr::variable("myfield"),
        ]);
        let variable_types = HashMap::from([("myfield", VarType::I64)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { int_i64: -8 }];
        let mut output = VariableValue { int_i64: 0 };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert_eq!(unsafe { output.int_i64 }, -12);
    }

    #[test]
    fn test_compile_add_coerces_integers_to_float() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("myfield"),
            UntypedExpr::literal(-2i64),
            UntypedExpr::literal(0.5f64),
        ]);
        let variable_types = HashMap::from([("myfield", VarType::U64)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { int_u64: 10 }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, 8.5);
    }

    #[test]
    fn test_compile_add_loads_multiple_variable_slots() {
        let untyped_expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("x"), UntypedExpr::variable("y")]);
        let variable_types = HashMap::from([("x", VarType::U64), ("y", VarType::F64)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { int_u64: 10 }, VariableValue { float: 0.5 }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, 10.5);
    }

    #[test]
    fn test_compile_u64_to_float_coercion_is_unsigned() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::literal(0.0f64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { int_u64: u64::MAX }];
        let mut output = VariableValue { float: 0.0 };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert_eq!(unsafe { output.float }, u64::MAX as f64);
    }

    #[test]
    fn test_compile_reuses_repeated_variable_slot() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            UntypedExpr::variable("x"),
            UntypedExpr::literal(1u64),
        ]);
        let variable_types = HashMap::from([("x", VarType::U64)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { int_u64: 4 }];
        let mut output = VariableValue { int_u64: 0 };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert_eq!(compiled_fn.input_vars.len(), 1);
        assert_eq!(unsafe { output.int_u64 }, 9);
    }

    #[test]
    fn test_compile_bool_variable() {
        let untyped_expr = UntypedExpr::variable("flag");
        let variable_types = HashMap::from([("flag", VarType::Bool)]);
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue { boolean: true }];
        let mut output = VariableValue { boolean: false };

        unsafe { compiled_fn.call(&input, &mut output) };

        assert!(unsafe { output.boolean });
    }

    #[test]
    fn test_compile_string_literal_keeps_descriptor_alive() {
        let untyped_expr = UntypedExpr::literal("hello");
        let compiled_fn = compile(&untyped_expr, &HashMap::new()).unwrap();
        drop(untyped_expr);
        let mut output = VariableValue {
            string: std::ptr::null_mut(),
        };

        unsafe { compiled_fn.call(&[], &mut output) };

        let output_string = unsafe { &*output.string };
        assert_eq!(unsafe { output_string.as_str() }, "hello");
    }

    #[test]
    fn test_compile_empty_add_uses_zero_identity() {
        let untyped_expr = Function::Add.call_untyped_expr(Vec::new());
        let compiled_fn = compile(&untyped_expr, &HashMap::new()).unwrap();
        let mut output = VariableValue { int_u64: 1 };

        unsafe { compiled_fn.call(&[], &mut output) };

        assert_eq!(unsafe { output.int_u64 }, 0);
    }
}
