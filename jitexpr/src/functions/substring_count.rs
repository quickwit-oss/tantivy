//! `SUBSTRING_COUNT(haystack, needle)` counts non-overlapping substring occurrences.
//!
//! Both arguments are strings and null propagates. Matching is case-sensitive and byte-based;
//! `SUBSTRING_COUNT("aaaa", "aa")` is 2. An empty needle returns zero rather than counting string
//! boundaries.

use std::collections::HashMap;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_substring_count";
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SubstringCountFnCall {
    args: Box<[TypedExpr]>,
}

impl FnCall for SubstringCountFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target: InferredTypeSet,
        inferred: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target.intersect(InferredTypeSet::I64).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::SubstringCount,
                expected: target,
                got: InferredTypeSet::I64,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::SubstringCount,
                expected: 2,
                got: args.len(),
            });
        }
        for arg in args {
            crate::ast::infer_types_aux(arg, InferredTypeSet::STRING, inferred)?;
        }
        Ok(InferredTypeSet::I64)
    }
    fn call_with_types(
        args: &[UntypedExpr],
        _target: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        let args = args
            .iter()
            .map(|arg| context.apply_types(arg, InferredTypeSet::STRING))
            .collect::<Result<Vec<_>, _>>()?;
        if args.iter().any(|arg| arg.return_type == VarType::None) {
            return Ok(TypedExpr::none());
        }
        Ok(TypedExpr {
            return_type: VarType::I64,
            ast: TypedExprAst::from_call(SubstringCountFnCall {
                args: args.into_boxed_slice(),
            }),
        })
    }
    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }
    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        crate::compile::format_function_call("SUBSTRING_COUNT", self.args.iter(), formatter)
    }
    fn emit_cranelift_ir(
        &self,
        _return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let haystack = context.compile_expr(&self.args[0], builder)?;
        let needle = context.compile_expr(&self.args[1], builder)?;
        let call = builder.ins().call(
            context.native_functions().substring_count(),
            &[
                haystack.value,
                haystack.string_len,
                needle.value,
                needle.string_len,
            ],
        );
        let value = builder.inst_results(call)[0];
        let is_present = builder.ins().band(haystack.is_present, needle.is_present);
        Ok(LoweredValue {
            value,
            is_present,
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}
pub(super) fn register_jit_symbol(builder: &mut JITBuilder) {
    builder.symbol(SYMBOL, substring_count as *const u8);
}
pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature.params.extend([
        AbiParam::new(pointer),
        AbiParam::new(types::I64),
        AbiParam::new(pointer),
        AbiParam::new(types::I64),
    ]);
    signature.returns.push(AbiParam::new(types::I64));
    let id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(id, function))
}
unsafe extern "C" fn substring_count(
    haystack: *const u8,
    haystack_len: usize,
    needle: *const u8,
    needle_len: usize,
) -> i64 {
    if needle_len == 0 {
        return 0;
    }
    let haystack = unsafe { std::slice::from_raw_parts(haystack, haystack_len) };
    let needle = unsafe { std::slice::from_raw_parts(needle, needle_len) };
    let mut count = 0i64;
    let mut rest = haystack;
    while needle.len() <= rest.len() {
        let Some(pos) = rest
            .windows(needle.len())
            .position(|window| window == needle)
        else {
            break;
        };
        count += 1;
        rest = &rest[pos + needle.len()..];
    }
    count
}
impl From<SubstringCountFnCall> for FnCallEnum {
    fn from(call: SubstringCountFnCall) -> Self {
        FnCallEnum::SubstringCount(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;
    fn eval(expr: &str) -> Option<i64> {
        let expr = deserialize(expr).unwrap();
        let mut compiled = compile(&expr, &HashMap::new()).unwrap();
        unsafe { compiled.call(&[]).as_i64() }
    }
    #[test]
    fn test_signature_and_counts() {
        assert!(matches!(
            infer_types(&deserialize("(SUBSTRING_COUNT \"a\")").unwrap()),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::SubstringCount,
                expected: 2,
                ..
            })
        ));
        assert_eq!(eval("(SUBSTRING_COUNT \"aaaa\" \"aa\")"), Some(2));
        assert_eq!(eval("(SUBSTRING_COUNT \"Abab\" \"ab\")"), Some(1));
        assert_eq!(eval("(SUBSTRING_COUNT \"abc\" \"\")"), Some(0));
    }
    #[test]
    fn test_runtime_null() {
        let expr = deserialize("(SUBSTRING_COUNT text needle)").unwrap();
        let mut compiled = compile(
            &expr,
            &HashMap::from([("text", VarType::Str), ("needle", VarType::Str)]),
        )
        .unwrap();
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::some("abc"), VariableValue::none()])
                    .as_i64()
            },
            None
        );
    }
}
