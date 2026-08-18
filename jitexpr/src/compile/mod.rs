mod compile_fn_builder;
mod compiled_fn;
mod error;
mod string_arena;
mod typed_expr;

use std::collections::HashMap;

pub(crate) use compile_fn_builder::CompileFnBuilder;
pub use compiled_fn::CompiledFn;
use cranelift::codegen::ir::{
    InstBuilder as _, MemFlagsData, Type, Value as CraneliftValue, types as cranelift_types,
};
use cranelift::frontend::FunctionBuilder;
pub use error::CompileError;
#[cfg(test)]
pub(crate) use string_arena::STRING_ARENA_CAPACITY;
pub(crate) use string_arena::StringArena;
pub use typed_expr::TypedVariable;
pub(crate) use typed_expr::{TypedExpr, TypedExprAst, TypedLiteral};

use crate::ast::UntypedExpr;
use crate::functions::NativeFunctions;
use crate::types::{VarType, VariablePrimitiveOpt, VariableValue};

pub fn compile(
    untyped_expr: &UntypedExpr,
    var_types: &HashMap<&str, VarType>,
) -> Result<CompiledFn, CompileError> {
    let mut builder = CompileFnBuilder::new(var_types);
    let typed_expr = builder.build_typed_expr(untyped_expr)?;
    builder.compile_typed_expr(typed_expr)
}

/// Compiles an expression and returns Cranelift's assembly listing for the host target.
pub fn compile_to_assembly(
    untyped_expr: &UntypedExpr,
    var_types: &HashMap<&str, VarType>,
) -> Result<String, CompileError> {
    let mut builder = CompileFnBuilder::new(var_types);
    let typed_expr = builder.build_typed_expr(untyped_expr)?;
    builder.compile_typed_expr_to_assembly(typed_expr)
}

pub(crate) struct LoweringContext<'a> {
    args_ptr: CraneliftValue,
    string_arena_ptr: CraneliftValue,
    string_arena_was_reset: bool,
    pointer_type: Type,
    native_functions: &'a NativeFunctions,
}

/// The two SSA values used to represent a nullable expression result.
#[derive(Clone, Copy)]
pub(crate) struct LoweredValue {
    pub(crate) value: CraneliftValue,
    pub(crate) is_present: CraneliftValue,
    pub(crate) string_len: CraneliftValue,
}

impl LoweringContext<'_> {
    pub(crate) fn compile_expr(
        &mut self,
        expression: &TypedExpr,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        match &expression.ast {
            TypedExprAst::Literal(literal) => Ok(lower_literal(literal, self, builder)),
            TypedExprAst::Variable(variable) => {
                let slot_offset = variable.variable_id * std::mem::size_of::<VariableValue>();
                let value_offset = slot_offset as i32;
                let second_word_offset =
                    (slot_offset + std::mem::offset_of!(VariablePrimitiveOpt, is_present)) as i32;
                let value = builder.ins().load(
                    cranelift_type(variable.r#type, self.pointer_type),
                    MemFlagsData::trusted(),
                    self.args_ptr,
                    value_offset,
                );
                let (is_present, string_len) = if variable.r#type == VarType::Str {
                    let string_len = builder.ins().load(
                        cranelift_types::I64,
                        MemFlagsData::trusted(),
                        self.args_ptr,
                        second_word_offset,
                    );
                    let is_present =
                        builder
                            .ins()
                            .icmp_imm_u(cranelift::prelude::IntCC::NotEqual, value, 0);
                    (is_present, string_len)
                } else {
                    let is_present = builder.ins().load(
                        cranelift_types::I8,
                        MemFlagsData::trusted(),
                        self.args_ptr,
                        second_word_offset,
                    );
                    let string_len = builder.ins().iconst(cranelift_types::I64, 0);
                    (is_present, string_len)
                };
                Ok(LoweredValue {
                    value,
                    is_present,
                    string_len,
                })
            }
            TypedExprAst::Coerce { target_type, expr } => {
                let source_type = expr.return_type;
                let lowered = self.compile_expr(expr, builder)?;
                let value = lower_coercion(lowered.value, source_type, *target_type, builder)?;
                Ok(LoweredValue {
                    value,
                    is_present: lowered.is_present,
                    string_len: lowered.string_len,
                })
            }
            TypedExprAst::FnCall(fn_call) => fn_call.lower(expression.return_type, self, builder),
        }
    }

    pub(crate) fn pointer_type(&self) -> Type {
        self.pointer_type
    }

    pub(crate) fn string_arena_ptr(&mut self, builder: &mut FunctionBuilder<'_>) -> CraneliftValue {
        if !self.string_arena_was_reset {
            let zero = builder.ins().iconst(cranelift_types::I64, 0);
            builder.ins().store(
                MemFlagsData::trusted(),
                zero,
                self.string_arena_ptr,
                StringArena::CURSOR_OFFSET,
            );
            self.string_arena_was_reset = true;
        }
        self.string_arena_ptr
    }

    pub(crate) fn native_functions(&self) -> &NativeFunctions {
        self.native_functions
    }
}

fn lower_literal(
    literal: &TypedLiteral,
    context: &mut LoweringContext<'_>,
    builder: &mut FunctionBuilder<'_>,
) -> LoweredValue {
    let value = match literal {
        TypedLiteral::None => builder.ins().iconst(cranelift_types::I64, 0),
        TypedLiteral::Bool(value) => builder.ins().iconst(cranelift_types::I8, i64::from(*value)),
        TypedLiteral::U64(value) => builder.ins().iconst(cranelift_types::I64, *value as i64),
        TypedLiteral::I64(value) => builder.ins().iconst(cranelift_types::I64, *value),
        TypedLiteral::F64(value) => {
            builder
                .ins()
                .f64const(cranelift::codegen::ir::immediates::Ieee64::with_bits(
                    value.to_bits(),
                ))
        }
        TypedLiteral::String(value) => {
            let string_ptr = value.as_ptr() as usize;
            builder
                .ins()
                .iconst(context.pointer_type, string_ptr as i64)
        }
    };
    let is_present = builder.ins().iconst(
        cranelift_types::I8,
        i64::from(!matches!(literal, TypedLiteral::None)),
    );
    let string_len = match literal {
        TypedLiteral::String(value) => builder
            .ins()
            .iconst(cranelift_types::I64, value.len() as i64),
        _ => builder.ins().iconst(cranelift_types::I64, 0),
    };
    LoweredValue {
        value,
        is_present,
        string_len,
    }
}

fn lower_coercion(
    value: CraneliftValue,
    source: VarType,
    target: VarType,
    builder: &mut FunctionBuilder<'_>,
) -> Result<CraneliftValue, CompileError> {
    let coerced = match (source, target) {
        (source, target) if source == target => value,
        (VarType::U64, VarType::F64) => builder.ins().fcvt_from_uint(cranelift_types::F64, value),
        (VarType::I64, VarType::F64) => builder.ins().fcvt_from_sint(cranelift_types::F64, value),
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

fn cranelift_type(var_type: VarType, pointer_type: Type) -> Type {
    match var_type {
        VarType::Bool => cranelift_types::I8,
        VarType::F64 => cranelift_types::F64,
        VarType::U64 | VarType::I64 | VarType::None => cranelift_types::I64,
        VarType::Str => pointer_type,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::{Function, UntypedExpr};
    use crate::types::{VarType, VariableValue};

    #[test]
    fn test_compile_bool_variable() {
        let untyped_expr = UntypedExpr::variable("flag");
        let variable_types = HashMap::from([("flag", VarType::Bool)]);
        let mut compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        assert!(compiled_fn.string_arena.allocate(1).is_some());
        let input = [VariableValue::some(true)];
        let output = unsafe { compiled_fn.call(&input) };

        assert_eq!(unsafe { output.as_bool() }, Some(true));
        assert_eq!(compiled_fn.string_arena.used_bytes(), 1);
    }

    #[test]
    fn test_compile_none_variable() {
        let untyped_expr = UntypedExpr::variable("value");
        let variable_types = HashMap::from([("value", VarType::U64)]);
        let mut compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input = [VariableValue::none()];
        assert_eq!(compiled_fn.result_type(), VarType::U64);
        let output = unsafe { compiled_fn.call(&input) };

        assert_eq!(unsafe { output.as_u64() }, None);
    }

    #[test]
    fn test_compile_string_literal_keeps_backing_data_alive() {
        let untyped_expr = UntypedExpr::literal("hello");
        let mut compiled_fn = compile(&untyped_expr, &HashMap::new()).unwrap();
        drop(untyped_expr);
        let output = unsafe { compiled_fn.call(&[]) };

        assert_eq!(unsafe { output.as_str() }, Some("hello"));
    }

    #[test]
    fn test_compile_returns_borrowed_string_variable_directly() {
        let untyped_expr = UntypedExpr::variable("value");
        let variable_types = HashMap::from([("value", VarType::Str)]);
        let mut compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let value = String::from("hello from an input");
        let input = [VariableValue::some(value.as_str())];
        let output = unsafe { compiled_fn.call(&input) };

        let output = unsafe { output.as_str() }.unwrap();
        assert_eq!(output, value);
        assert_eq!(output.as_ptr(), value.as_ptr());
    }

    #[test]
    fn test_compile_none_literal_returns_absent_value() {
        let untyped_expr = UntypedExpr::literal(crate::ast::Literal::None);
        let mut compiled_fn = compile(&untyped_expr, &HashMap::new()).unwrap();
        assert_eq!(compiled_fn.result_type(), VarType::None);
        let output = unsafe { compiled_fn.call(&[]) };

        assert_eq!(unsafe { output.as_u64() }, None);
    }

    #[test]
    fn test_compile_to_assembly() {
        let untyped_expr = UntypedExpr::variable("value");
        let variable_types = HashMap::from([("value", VarType::F64)]);

        let assembly = compile_to_assembly(&untyped_expr, &variable_types).unwrap();

        assert!(assembly.contains("block0:"));
        assert!(!assembly.trim().is_empty());
    }

    #[test]
    fn test_compile_native_call_to_assembly() {
        let untyped_expr = Function::RegexpExtract.call_untyped_expr(vec![
            UntypedExpr::variable("message"),
            UntypedExpr::literal("([a-z]+)"),
            UntypedExpr::literal(0u64),
        ]);
        let variable_types = HashMap::from([("message", VarType::Str)]);

        let assembly = compile_to_assembly(&untyped_expr, &variable_types).unwrap();

        assert!(assembly.contains("block0:"));
        assert!(!assembly.trim().is_empty());
    }
}
