mod compile_fn_builder;
mod compiled_fn;
mod error;
mod typed_expr;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem::size_of;

pub(crate) use compile_fn_builder::{CompileFnBuilder, RegexRef};
pub use compiled_fn::CompiledFn;
use cranelift::codegen::ir::MemFlagsData;
use cranelift::prelude::*;
pub use error::CompileError;
pub use typed_expr::TypedVariable;
pub(crate) use typed_expr::{TypedExpr, TypedExprAst, TypedLiteral};

use crate::ast::UntypedExpr;
use crate::functions::NativeFunctions;
use crate::types::{StringRef, VarType, VariableValue};

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
    args_ptr: Value,
    regexes_ptr: Value,
    pointer_type: Type,
    regex_match_results: &'a [UnsafeCell<StringRef>],
    native_functions: &'a NativeFunctions,
}

impl LoweringContext<'_> {
    pub(crate) fn compile_expr(
        &mut self,
        expression: &TypedExpr,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<Value, CompileError> {
        match &expression.ast {
            TypedExprAst::Literal(literal) => Ok(lower_literal(literal, self, builder)),
            TypedExprAst::Variable(variable) => {
                let byte_offset = variable
                    .variable_id
                    .checked_mul(size_of::<VariableValue>())
                    .and_then(|offset| i32::try_from(offset).ok())
                    .ok_or(CompileError::InputOffsetOverflow {
                        variable_id: variable.variable_id,
                    })?;
                Ok(builder.ins().load(
                    cranelift_type(variable.r#type, self.pointer_type),
                    MemFlagsData::trusted(),
                    self.args_ptr,
                    byte_offset,
                ))
            }
            TypedExprAst::Coerce { target_type, expr } => {
                let source_type = expr.return_type;
                let value = self.compile_expr(expr, builder)?;
                lower_coercion(value, source_type, *target_type, builder)
            }
            TypedExprAst::FnCall(fn_call) => fn_call.lower(expression.return_type, self, builder),
        }
    }

    pub(crate) fn pointer_type(&self) -> Type {
        self.pointer_type
    }

    pub(crate) fn regexes_ptr(&self) -> Value {
        self.regexes_ptr
    }

    pub(crate) fn native_functions(&self) -> &NativeFunctions {
        self.native_functions
    }

    pub(crate) fn regex_match_result(&self, regex_ref: RegexRef) -> *mut StringRef {
        self.regex_match_results[regex_ref.index()].get()
    }
}

fn lower_literal(
    literal: &TypedLiteral,
    context: &mut LoweringContext<'_>,
    builder: &mut FunctionBuilder<'_>,
) -> Value {
    match literal {
        TypedLiteral::None => builder.ins().iconst(types::I64, 0),
        TypedLiteral::Bool(value) => builder.ins().iconst(types::I8, i64::from(*value)),
        TypedLiteral::U64(value) => builder.ins().iconst(types::I64, *value as i64),
        TypedLiteral::I64(value) => builder.ins().iconst(types::I64, *value),
        TypedLiteral::F64(value) => builder.ins().f64const(Ieee64::with_bits(value.to_bits())),
        TypedLiteral::String(string_ref) => {
            let string_ref_ptr = (string_ref as *const StringRef) as usize;
            builder
                .ins()
                .iconst(context.pointer_type, string_ref_ptr as i64)
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
