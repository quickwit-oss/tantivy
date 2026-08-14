use std::collections::HashMap;

use cranelift_jit::JITModule;

use crate::ast::{TypedExpr, TypedVariable, UntypedExpr, apply_types};
use crate::types::{VarType, VariableValue};

/// An expression compiled to native machine code.
///
/// This object owns the JIT module containing its executable memory.
pub struct CompiledFunction {
    pub(crate) entry: JitEntry,
    pub(crate) _module: JITModule,
    pub input_vars: Vec<TypedVariable>,
    pub typed_expr: TypedExpr,
}

impl CompiledFunction {
    pub unsafe fn call(&self, args: &[VariableValue], result: &mut VariableValue) {
        debug_assert_eq!(args.len(), self.input_vars.len());
        // SAFETY: Guaranteed by the caller.
        (self.entry)(args.as_ptr(), result);
    }
}

type JitEntry = unsafe extern "C" fn(*const VariableValue, *mut VariableValue);

#[derive(Debug, thiserror::Error)]
pub enum CompileError {}

pub fn compile(
    untyped_expr: &UntypedExpr,
    var_types: &HashMap<&str, VarType>,
) -> Result<CompiledFunction, CompileError> {
    let (typed_expr, input_vars) = apply_types(&untyped_expr, var_types);
    compile_typed_expr(typed_expr, input_vars)
}

fn compile_typed_expr(
    expression: TypedExpr,
    input_vars: Vec<TypedVariable>,
) -> Result<CompiledFunction, CompileError> {
    todo!();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::{Function, UntypedExpr};
    use crate::types::VarType;

    #[test]
    fn test_compile_simple() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1u64),
            UntypedExpr::variable("myfield"),
        ]);
        let variable_types: HashMap<&str, VarType> =
            std::iter::once(("myfield", VarType::U64)).collect();
        let compiled_fn = compile(&untyped_expr, &variable_types).unwrap();
        let input: Box<[VariableValue]> = vec![VariableValue { int_u64: 2u64 }].into_boxed_slice();
        let mut output: VariableValue = VariableValue { int_u64: 0u64 };
        unsafe { compiled_fn.call(&input[..], &mut output) };
        assert_eq!(unsafe { output.int_u64 }, 3u64);
    }
}
