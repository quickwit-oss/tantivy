//! `TRIM` removes a whole delimiter once from selected ends of a string.
//!
//! It takes `(input, delimiter, mode)`, where the latter two arguments are string literals emitted
//! by the calculated-field producer. Mode is case-insensitive `leading`, `trailing`, or `both`.
//! Unlike SQL character-set trimming, `"xy"` is removed only as one complete prefix/suffix and at
//! most once per selected side. Null input propagates; an empty delimiter leaves the input intact.

use std::collections::HashMap;
use std::sync::Arc;

use cranelift::codegen::ir::{FuncRef, Function as CraneliftFunction, Type, types};
use cranelift::frontend::FunctionBuilder;
use cranelift::prelude::{AbiParam, InstBuilder, IntCC};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

use crate::ast::{Function, InferredTypeSet, Literal, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

const SYMBOL: &str = "jitexpr_string_trim";

#[derive(Clone, Copy, Debug, PartialEq)]
enum TrimMode {
    Leading,
    Trailing,
    Both,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TrimFnCall {
    input: Box<TypedExpr>,
    delimiter: Arc<str>,
    mode: TrimMode,
}

impl FnCall for TrimFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        if target_type.intersect(InferredTypeSet::STRING).is_none() {
            return Err(TypeError::WrongFunctionReturnType {
                function: Function::Trim,
                expected: target_type,
                got: InferredTypeSet::STRING,
            });
        }
        if args.len() != 3 {
            return Err(TypeError::InvalidNumberOfArguments {
                function: Function::Trim,
                expected: 3,
                got: args.len(),
            });
        }
        for arg in args {
            crate::ast::infer_types_aux(arg, InferredTypeSet::STRING, inferred_types)?;
        }
        Ok(InferredTypeSet::STRING)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        _target: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 3, "expected 3 args for TRIM");
        let input = context.apply_types(&args[0], InferredTypeSet::STRING)?;
        if input.return_type == VarType::None {
            return Ok(TypedExpr::none());
        }
        let UntypedExpr::Literal(Literal::String(delimiter)) = &args[1] else {
            panic!("TRIM delimiter must be a string literal")
        };
        let UntypedExpr::Literal(Literal::String(mode)) = &args[2] else {
            panic!("TRIM mode must be a string literal")
        };
        let mode = if mode.eq_ignore_ascii_case("leading") {
            TrimMode::Leading
        } else if mode.eq_ignore_ascii_case("trailing") {
            TrimMode::Trailing
        } else if mode.eq_ignore_ascii_case("both") {
            TrimMode::Both
        } else {
            panic!("TRIM mode must be leading, trailing, or both")
        };
        Ok(TypedExpr {
            return_type: VarType::Str,
            ast: TypedExprAst::from_call(TrimFnCall {
                input: Box::new(input),
                delimiter: Arc::clone(delimiter),
                mode,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        std::slice::from_mut(&mut self.input)
    }

    fn emit_cranelift_ir(
        &self,
        _return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        let input = context.compile_expr(&self.input, builder)?;
        let null = builder.ins().iconst(context.pointer_type(), 0);
        let input_ptr = builder.ins().select(input.is_present, input.value, null);
        let delimiter_ptr = builder
            .ins()
            .iconst(context.pointer_type(), self.delimiter.as_ptr() as i64);
        let delimiter_len = builder
            .ins()
            .iconst(types::I64, self.delimiter.len() as i64);
        let mode = builder.ins().iconst(types::I64, self.mode as i64);
        let call = builder.ins().call(
            context.native_functions().string_trim(),
            &[
                input_ptr,
                input.string_len,
                delimiter_ptr,
                delimiter_len,
                mode,
            ],
        );
        let value = builder.inst_results(call)[0];
        let string_len = builder.inst_results(call)[1];
        let is_present = builder.ins().icmp_imm_u(IntCC::NotEqual, value, 0);
        Ok(LoweredValue {
            value,
            is_present,
            string_len,
        })
    }
}

pub(super) fn register_jit_symbol(builder: &mut JITBuilder) {
    builder.symbol(SYMBOL, string_trim as *const u8);
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
        AbiParam::new(types::I64),
    ]);
    signature
        .returns
        .extend([AbiParam::new(pointer_type), AbiParam::new(types::I64)]);
    let id = module.declare_function(SYMBOL, Linkage::Import, &signature)?;
    Ok(module.declare_func_in_func(id, function))
}

#[repr(C)]
struct RawStr {
    ptr: *const u8,
    len: usize,
}
unsafe extern "C" fn string_trim(
    input_ptr: *const u8,
    input_len: usize,
    delimiter_ptr: *const u8,
    delimiter_len: usize,
    mode: u64,
) -> RawStr {
    if input_ptr.is_null() {
        return RawStr {
            ptr: std::ptr::null(),
            len: 0,
        };
    }
    let input =
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(input_ptr, input_len)) };
    let delimiter = unsafe {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(delimiter_ptr, delimiter_len))
    };
    let leading = mode == TrimMode::Leading as u64 || mode == TrimMode::Both as u64;
    let trailing = mode == TrimMode::Trailing as u64 || mode == TrimMode::Both as u64;
    let value = if leading {
        input.strip_prefix(delimiter).unwrap_or(input)
    } else {
        input
    };
    let value = if trailing {
        value.strip_suffix(delimiter).unwrap_or(value)
    } else {
        value
    };
    RawStr {
        ptr: value.as_ptr(),
        len: value.len(),
    }
}

impl From<TrimFnCall> for FnCallEnum {
    fn from(call: TrimFnCall) -> Self {
        FnCallEnum::Trim(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expr: &str) -> Option<String> {
        let expr = deserialize(expr).unwrap();
        let mut compiled = compile(&expr, &HashMap::new()).unwrap();
        unsafe { compiled.call(&[]).as_str().map(str::to_owned) }
    }

    #[test]
    fn test_signature_and_modes() {
        assert!(infer_types(&deserialize("(TRIM value \"xy\" \"both\")").unwrap()).is_ok());
        assert!(matches!(
            infer_types(&deserialize("(TRIM value)").unwrap()),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Trim,
                expected: 3,
                ..
            })
        ));
        assert_eq!(
            eval("(TRIM \"xyhelloxy\" \"xy\" \"both\")"),
            Some("hello".into())
        );
        assert_eq!(
            eval("(TRIM \"xyxy\" \"xy\" \"leading\")"),
            Some("xy".into())
        );
        assert_eq!(
            eval("(TRIM \"xyxy\" \"xy\" \"TRAILING\")"),
            Some("xy".into())
        );
        assert_eq!(eval("(TRIM \"hello\" \"\" \"both\")"), Some("hello".into()));
    }

    #[test]
    fn test_runtime_null_and_unicode() {
        let expr = deserialize("(TRIM value \"é\" \"both\")").unwrap();
        let mut compiled = compile(&expr, &HashMap::from([("value", VarType::Str)])).unwrap();
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::some("éhelloé")]).as_str() },
            Some("hello")
        );
        assert_eq!(
            unsafe { compiled.call(&[VariableValue::none()]).as_str() },
            None
        );
    }
}
