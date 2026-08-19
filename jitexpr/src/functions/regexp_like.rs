//! `REGEXP_LIKE(input, pattern)` tests whether a regular expression matches anywhere in a string.
//!
//! The pattern must be constant. Before compilation, the placeholder ICU converter removes
//! Java-style named-group names (`(?<name>` becomes `(`) and changes atomic groups (`(?>` becomes
//! `(?:`). Invalid patterns fail compilation. Null input and definitively non-string input both
//! return present `false`; this deliberate mismatch behavior is different from ordinary strict
//! functions.

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

const SYMBOL: &str = "jitexpr_regexp_like";
#[derive(Clone, Debug)]
pub(crate) struct RegexpLikeFnCall {
    regex: Arc<Regex>,
    input: Box<TypedExpr>,
}
impl PartialEq for RegexpLikeFnCall {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str() && self.input == other.input
    }
}

fn convert_pattern(pattern: &str) -> String {
    let named = Regex::new(r"\(\?<[^>]+>").expect("static regex");
    named.replace_all(pattern, "(").replace("(?>", "(?:")
}

impl FnCall for RegexpLikeFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target: InferredTypeSet,
        inferred: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target.intersect(InferredTypeSet::BOOLEAN).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::RegexpLike,
                expected: target,
                got: InferredTypeSet::BOOLEAN,
            });
        }
        if args.len() != 2 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::RegexpLike,
                expected: 2,
                got: args.len(),
            });
        }
        crate::ast::infer_types_aux(&args[0], InferredTypeSet::ALL, inferred)?;
        crate::ast::infer_types_aux(&args[1], InferredTypeSet::STRING, inferred)?;
        Ok(InferredTypeSet::BOOLEAN)
    }
    fn call_with_types(
        args: &[UntypedExpr],
        _target: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for REGEXP_LIKE");
        let input_target = match &args[0] {
            UntypedExpr::Literal(literal) => InferredTypeSet::singleton(literal.r#type()),
            _ => InferredTypeSet::ALL,
        };
        let input = context.apply_types(&args[0], input_target)?;
        let UntypedExpr::Literal(Literal::String(pattern)) = &args[1] else {
            panic!("REGEXP_LIKE pattern must be a string literal")
        };
        let converted = convert_pattern(pattern);
        let regex =
            Arc::new(
                Regex::new(&converted).map_err(|source| CompileError::InvalidRegex {
                    pattern: pattern.to_string(),
                    source,
                })?,
            );
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(RegexpLikeFnCall {
                regex,
                input: Box::new(input),
            }),
        })
    }
    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.input)
    }
    fn serialize(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "REGEXP_LIKE {} ", self.input)?;
        crate::compile::format_string_literal(self.regex.as_str(), formatter)
    }
    fn emit_cranelift_ir(
        &self,
        _return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        if self.input.return_type != VarType::Str {
            return Ok(LoweredValue {
                value: builder.ins().iconst(types::I8, 0),
                is_present: builder.ins().iconst(types::I8, 1),
                string_len: builder.ins().iconst(types::I64, 0),
            });
        }
        let input = context.compile_expr(&self.input, builder)?;
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let input_ptr = builder.ins().select(input.is_present, input.value, null);
        let regex_ptr = builder
            .ins()
            .iconst(context.pointer_type(), Arc::as_ptr(&self.regex) as i64);
        let call = builder.ins().call(
            context.native_functions().regexp_like(),
            &[regex_ptr, input_ptr, input.string_len],
        );
        Ok(LoweredValue {
            value: builder.inst_results(call)[0],
            is_present: builder.ins().iconst(types::I8, 1),
            string_len: builder.ins().iconst(types::I64, 0),
        })
    }
}
pub(super) fn register_jit_symbol(builder: &mut JITBuilder) {
    builder.symbol(SYMBOL, regexp_like as *const u8);
}
pub(super) fn declare_native_function(
    module: &mut JITModule,
    function: &mut CraneliftFunction,
    pointer: Type,
) -> Result<FuncRef, CompileError> {
    let mut signature = module.make_signature();
    signature.params.extend([
        AbiParam::new(pointer),
        AbiParam::new(pointer),
        AbiParam::new(types::I64),
    ]);
    signature.returns.push(AbiParam::new(types::I8));
    let id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(id, function))
}
unsafe extern "C" fn regexp_like(regex: *const Regex, input: *const u8, len: usize) -> u8 {
    if input.is_null() {
        return 0;
    }
    let regex = unsafe { &*regex };
    let input = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input, len)) };
    u8::from(regex.is_match(input))
}
impl From<RegexpLikeFnCall> for FnCallEnum {
    fn from(call: RegexpLikeFnCall) -> Self {
        FnCallEnum::RegexpLike(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;
    fn eval(expr: &str) -> Option<bool> {
        let expr = deserialize(expr).unwrap();
        let mut compiled = compile(&expr, &HashMap::new()).unwrap();
        unsafe { compiled.call(&[]).as_bool() }
    }
    #[test]
    fn test_signature_matching_and_conversion() {
        assert!(matches!(
            infer_types(&deserialize("(REGEXP_LIKE \"a\")").unwrap()),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::RegexpLike,
                expected: 2,
                ..
            })
        ));
        assert_eq!(eval("(REGEXP_LIKE \"prefix-123\" \"[0-9]+\")"), Some(true));
        assert_eq!(eval("(REGEXP_LIKE \"abc\" \"^z\")"), Some(false));
        assert_eq!(eval("(REGEXP_LIKE \"abc\" \"(?<word>abc)\")"), Some(true));
    }
    #[test]
    fn test_null_and_non_string_are_false() {
        assert_eq!(eval("(REGEXP_LIKE none \"x\")"), Some(false));
        assert_eq!(eval("(REGEXP_LIKE 123i64 \"123\")"), Some(false));
        let expr = deserialize("(REGEXP_LIKE value \"x\")").unwrap();
        let mut compiled = compile(&expr, &HashMap::from([("value", VarType::Str)])).unwrap();
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_bool() },
            Some(false)
        );
    }
}
