use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use cranelift::codegen::Context as CodegenContext;
use cranelift::codegen::control::ControlPlane;
use cranelift::codegen::ir::{MemFlagsData, UserFuncName};
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Module, ModuleError, default_libcall_names};
use regex::Regex;

use super::compiled_fn::JitEntry;
use super::{
    CompileError, CompiledFn, LoweringContext, TypedExpr, TypedExprAst, TypedLiteral, TypedVariable,
};
use crate::ast::{InferredTypeSet, Literal, UntypedExpr};
use crate::functions::{declare_native_functions, register_jit_symbols};
use crate::types::{StringRef, VarType};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegexRef(usize);

impl RegexRef {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

pub(crate) struct CompileFnBuilder<'types, 'names> {
    variable_types: &'types HashMap<&'names str, VarType>,
    input_vars: Vec<TypedVariable>,
    regexes: Vec<Regex>,
    regex_match_results: Vec<UnsafeCell<StringRef<'static>>>,
    string_literals: Vec<Arc<str>>,
}

struct LoweredFunction {
    module: JITModule,
    context: CodegenContext,
    function_id: FuncId,
    input_vars: Vec<TypedVariable>,
    regexes: Box<[Regex]>,
    regex_match_results: Box<[UnsafeCell<StringRef<'static>>]>,
    string_literals: Box<[Arc<str>]>,
    expression: Box<TypedExpr>,
}

impl<'types, 'names> CompileFnBuilder<'types, 'names> {
    pub(crate) fn new(variable_types: &'types HashMap<&'names str, VarType>) -> Self {
        CompileFnBuilder {
            variable_types,
            input_vars: Vec::new(),
            regexes: Vec::new(),
            regex_match_results: Vec::new(),
            string_literals: Vec::new(),
        }
    }

    pub(crate) fn variable_types(&self) -> &HashMap<&'names str, VarType> {
        self.variable_types
    }

    pub(crate) fn register_regex(&mut self, regex: Regex) -> RegexRef {
        let regex_ref = RegexRef(self.regexes.len());
        self.regexes.push(regex);
        self.regex_match_results
            .push(UnsafeCell::new(StringRef::new("")));
        regex_ref
    }

    pub(crate) fn register_string_literal(&mut self, value: Arc<str>) -> StringRef<'static> {
        let value_ptr = Arc::as_ptr(&value);
        self.string_literals.push(value);
        // SAFETY: `value_ptr` points into the Arc now owned by
        // `self.string_literals`. The fake `'static` lifetime is only used in
        // the internal typed AST; `CompiledFn::call` narrows it to the borrow
        // of the compiled function before exposing it to callers.
        StringRef::new(unsafe { &*value_ptr })
    }

    /// If a variable is missing from `variable_types`, it is treated as `None`.
    pub(crate) fn build_typed_expr(
        &mut self,
        untyped_expr: &UntypedExpr,
    ) -> Result<TypedExpr, CompileError> {
        let mut typed_expr = self.apply_types(untyped_expr, InferredTypeSet::ALL)?;
        self.assign_variable_ids(&mut typed_expr);
        Ok(typed_expr)
    }

    pub(crate) fn assign_variable_ids(&mut self, typed_expr: &mut TypedExpr) {
        self.input_vars = assign_variable_ids(typed_expr);
    }

    fn apply_literal_type(
        &mut self,
        literal: &Literal,
        target_type_set: InferredTypeSet,
    ) -> TypedLiteral {
        if literal.is_none() {
            return TypedLiteral::None;
        }
        let inferred_type_set = literal.types();
        let intersection = inferred_type_set.intersect(target_type_set);

        if intersection.contains(VarType::Bool) {
            match literal {
                Literal::Bool(value) => TypedLiteral::Bool(*value),
                _ => panic!("cannot coerce literal {literal:?} to bool"),
            }
        } else if intersection.contains(VarType::I64) {
            match literal {
                Literal::U64(value) => TypedLiteral::I64(*value as i64),
                Literal::I64(value) => TypedLiteral::I64(*value),
                Literal::F64(value) if f64_to_i64_lossless(*value).is_some() => {
                    TypedLiteral::I64(f64_to_i64_lossless(*value).unwrap())
                }
                _ => panic!("cannot coerce literal {literal:?} to i64"),
            }
        } else if intersection.contains(VarType::U64) {
            match literal {
                Literal::U64(value) => TypedLiteral::U64(*value),
                Literal::I64(value) => TypedLiteral::U64(*value as u64),
                Literal::F64(value) if f64_to_u64_lossless(*value).is_some() => {
                    TypedLiteral::U64(f64_to_u64_lossless(*value).unwrap())
                }
                _ => panic!("cannot coerce literal {literal:?} to u64"),
            }
        } else if intersection.contains(VarType::F64) {
            match literal {
                Literal::U64(value) => TypedLiteral::F64(*value as f64),
                Literal::I64(value) => TypedLiteral::F64(*value as f64),
                Literal::F64(value) => TypedLiteral::F64(*value),
                _ => panic!("cannot coerce literal {literal:?} to f64"),
            }
        } else if intersection.contains(VarType::Str) {
            match literal {
                Literal::String(value) => {
                    TypedLiteral::String(self.register_string_literal(value.clone()))
                }
                _ => panic!("cannot coerce literal {literal:?} to string"),
            }
        } else if intersection.contains(VarType::None) {
            match literal {
                Literal::None => TypedLiteral::None,
                _ => panic!("cannot coerce literal {literal:?} to none"),
            }
        } else {
            panic!(
                "no compatible type for literal {literal:?} with target type set \
                 {target_type_set:?}"
            )
        }
    }

    pub(crate) fn apply_types(
        &mut self,
        untyped_expr: &UntypedExpr,
        target_type_set: InferredTypeSet,
    ) -> Result<TypedExpr, CompileError> {
        match untyped_expr {
            UntypedExpr::Literal(literal) => {
                let typed_literal = self.apply_literal_type(literal, target_type_set);
                let return_type = typed_literal.r#type();
                Ok(TypedExpr {
                    return_type,
                    ast: TypedExprAst::Literal(typed_literal),
                })
            }
            UntypedExpr::Variable(variable_name) => {
                let variable_type = self
                    .variable_types
                    .get(variable_name.as_ref())
                    .copied()
                    .unwrap_or(VarType::None);
                if variable_type == VarType::None {
                    Ok(TypedExpr {
                        return_type: VarType::None,
                        ast: TypedExprAst::Literal(TypedLiteral::None),
                    })
                } else {
                    let typed_expr = TypedExpr {
                        return_type: variable_type,
                        ast: TypedExprAst::variable(variable_name, variable_type),
                    };
                    if target_type_set.contains(variable_type) {
                        Ok(typed_expr)
                    } else if let Some(target_type) = preferred_numerical_type(target_type_set)
                        && is_numerical(variable_type)
                    {
                        Ok(typed_expr.coerce(target_type))
                    } else {
                        Ok(typed_expr)
                    }
                }
            }
            UntypedExpr::Call { function, args } => {
                function.call_with_types(args, target_type_set, self)
            }
        }
    }

    pub(super) fn compile_typed_expr(
        self,
        expression: TypedExpr,
    ) -> Result<CompiledFn, CompileError> {
        self.lower_typed_expr(expression)?.into_compiled_fn()
    }

    pub(super) fn compile_typed_expr_to_assembly(
        self,
        expression: TypedExpr,
    ) -> Result<String, CompileError> {
        self.lower_typed_expr(expression)?.into_assembly()
    }

    fn lower_typed_expr(self, expression: TypedExpr) -> Result<LoweredFunction, CompileError> {
        let CompileFnBuilder {
            input_vars,
            regexes,
            regex_match_results,
            string_literals,
            ..
        } = self;
        let regexes = regexes.into_boxed_slice();
        let regex_match_results = regex_match_results.into_boxed_slice();
        let string_literals = string_literals.into_boxed_slice();
        let expression = Box::new(expression);

        let mut jit_builder =
            JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())?;
        register_jit_symbols(&mut jit_builder);
        let mut module = JITModule::new(jit_builder);
        let target_config = module.target_config();
        let pointer_type = target_config.pointer_type();

        // The native entry point mirrors JitEntry: the two arguments point to the
        // input slots and CompiledFn::regexes. VariableOpt is returned as two
        // integer-class values according to the native C ABI.
        let mut signature = module.make_signature();
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(pointer_type));
        signature.returns.push(AbiParam::new(types::I64));
        signature.returns.push(AbiParam::new(types::I8));
        let function_id = module.declare_anonymous_function(&signature)?;

        let mut context = module.make_context();
        context.func.signature = signature;
        context.func.name = UserFuncName::user(0, function_id.as_u32());
        let native_functions =
            declare_native_functions(&mut module, &mut context.func, pointer_type)?;

        let mut function_builder_context = FunctionBuilderContext::new();
        {
            let mut builder =
                FunctionBuilder::new(&mut context.func, &mut function_builder_context);
            let entry_block = builder.create_block();
            builder.append_block_params_for_function_params(entry_block);
            builder.switch_to_block(entry_block);
            builder.seal_block(entry_block);

            let args_ptr = builder.block_params(entry_block)[0];
            let regexes_ptr = builder.block_params(entry_block)[1];
            let mut lowering_context = LoweringContext {
                args_ptr,
                regexes_ptr,
                pointer_type,
                regex_match_results: &regex_match_results,
                native_functions: &native_functions,
            };
            let lowered = lowering_context.compile_expr(&expression, &mut builder)?;
            let value_bits = match expression.return_type {
                VarType::Bool => builder.ins().uextend(types::I64, lowered.value),
                VarType::F64 => {
                    builder
                        .ins()
                        .bitcast(types::I64, MemFlagsData::new(), lowered.value)
                }
                VarType::U64 | VarType::I64 | VarType::Str | VarType::None => lowered.value,
            };
            builder.ins().return_(&[value_bits, lowered.is_present]);
            builder.finalize(target_config);
        }

        Ok(LoweredFunction {
            module,
            context,
            function_id,
            input_vars,
            regexes,
            regex_match_results,
            string_literals,
            expression,
        })
    }
}

fn preferred_numerical_type(inferred_types: InferredTypeSet) -> Option<VarType> {
    if inferred_types.i64 {
        Some(VarType::I64)
    } else if inferred_types.u64 {
        Some(VarType::U64)
    } else if inferred_types.f64 {
        Some(VarType::F64)
    } else {
        None
    }
}

fn is_numerical(var_type: VarType) -> bool {
    matches!(var_type, VarType::I64 | VarType::U64 | VarType::F64)
}

impl LoweredFunction {
    fn into_compiled_fn(self) -> Result<CompiledFn, CompileError> {
        let LoweredFunction {
            mut module,
            mut context,
            function_id,
            input_vars,
            regexes,
            regex_match_results,
            string_literals,
            expression,
        } = self;

        module.define_function(function_id, &mut context)?;
        module.finalize_definitions()?;

        let code = module.get_finalized_function(function_id);
        // SAFETY: `code` is the finalized entry point for the function whose ABI
        // was built above to exactly match `JitEntry`. The module is retained by
        // `CompiledFn`, so its executable allocation outlives `entry`.
        let entry = unsafe { mem::transmute::<*const u8, JitEntry>(code) };
        Ok(CompiledFn {
            entry,
            _module: module,
            _string_literals: string_literals,
            regexes,
            _regex_match_results: regex_match_results,
            inputs: input_vars,
            _typed_expr: expression,
        })
    }

    fn into_assembly(mut self) -> Result<String, CompileError> {
        self.context.set_disasm(true);
        let compiled_code = self
            .context
            .compile(self.module.isa(), &mut ControlPlane::default())
            .map_err(ModuleError::from)?;
        Ok(compiled_code
            .vcode
            .clone()
            .expect("Cranelift assembly was requested before compilation"))
    }
}

/// Converts an `f64` to an `i64` only when the value can be represented exactly.
fn f64_to_i64_lossless(value: f64) -> Option<i64> {
    let is_integral = value.is_finite() && value.fract() == 0.0;
    if is_integral && value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
        Some(value as i64)
    } else {
        None
    }
}

/// Converts an `f64` to a `u64` only when the value can be represented exactly.
fn f64_to_u64_lossless(value: f64) -> Option<u64> {
    let is_integral = value.is_finite() && value.fract() == 0.0;
    if is_integral && value >= 0.0 && value < u64::MAX as f64 {
        Some(value as u64)
    } else {
        None
    }
}

fn assign_variable_ids(expr: &mut TypedExpr) -> Vec<TypedVariable> {
    let mut name_to_vars: HashMap<Arc<str>, TypedVariable> = HashMap::new();
    assign_variable_ids_aux(&mut expr.ast, &mut name_to_vars);
    let mut input_vars: Vec<TypedVariable> = name_to_vars.into_values().collect();
    input_vars.sort_by_key(|var| var.variable_id);
    input_vars
}

fn assign_variable_ids_aux(
    ast: &mut TypedExprAst,
    name_to_vars: &mut HashMap<Arc<str>, TypedVariable>,
) {
    match ast {
        TypedExprAst::Literal(_) => {}
        TypedExprAst::Variable(var) => {
            if let Some(typed_var) = name_to_vars.get(&var.variable_name) {
                assert_eq!(
                    typed_var.r#type, var.r#type,
                    "variable `{}` appears with two different types (`{:?}` and `{:?}`); a typed \
                     expr AST must be built with a single explicit type per variable",
                    var.variable_name, typed_var.r#type, var.r#type,
                );
                var.variable_id = typed_var.variable_id;
            } else {
                var.variable_id = name_to_vars.len();
                name_to_vars.insert(var.variable_name.clone(), var.clone());
            };
        }
        TypedExprAst::Coerce { expr, .. } => {
            assign_variable_ids_aux(&mut expr.ast, name_to_vars);
        }
        TypedExprAst::FnCall(fn_call) => {
            for arg in fn_call.args_mut() {
                assign_variable_ids_aux(&mut arg.ast, name_to_vars);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::ast::Function;
    use crate::functions::{AddFnCall, FnCallEnum};

    #[test]
    fn test_apply_types_to_literal() {
        let untyped_expr = UntypedExpr::literal("hello");
        let variable_types = HashMap::new();
        let mut builder = CompileFnBuilder::new(&variable_types);
        let typed_expr = builder.build_typed_expr(&untyped_expr).unwrap();

        assert_eq!(typed_expr.return_type, VarType::Str);
        let TypedExprAst::Literal(TypedLiteral::String(string_ref)) = typed_expr.ast else {
            panic!("expected a typed string literal");
        };
        assert_eq!(string_ref.as_str(), "hello");
    }

    #[test]
    fn test_assign_variable_ids_two_variables_different_types() {
        let untyped_expr = Function::Add
            .call_untyped_expr(vec![UntypedExpr::variable("x"), UntypedExpr::variable("y")]);
        let variable_types = HashMap::from([("x", VarType::U64), ("y", VarType::F64)]);
        let mut builder = CompileFnBuilder::new(&variable_types);

        let _typed_expr = builder.build_typed_expr(&untyped_expr).unwrap();
        let var_args = &builder.input_vars;

        assert_eq!(var_args.len(), 2);
        assert_eq!(var_args[0].variable_name.as_ref(), "x");
        assert_eq!(var_args[0].r#type, VarType::U64);
        assert_eq!(var_args[0].variable_id, 0);
        assert_eq!(var_args[1].variable_name.as_ref(), "y");
        assert_eq!(var_args[1].r#type, VarType::F64);
        assert_eq!(var_args[1].variable_id, 1);
    }

    #[test]
    #[should_panic(expected = "appears with two different types")]
    fn test_assign_variable_ids_panics_on_inconsistent_types() {
        let mut typed_expr = TypedExpr {
            return_type: VarType::F64,
            ast: TypedExprAst::FnCall(FnCallEnum::Add(AddFnCall {
                args: vec![
                    TypedExprAst::variable("x", VarType::U64).with_type(VarType::U64),
                    TypedExprAst::variable("x", VarType::F64).with_type(VarType::F64),
                ]
                .into_boxed_slice(),
            })),
        };

        assign_variable_ids(&mut typed_expr);
    }

    #[test]
    fn test_assign_variable_ids_dedups_repeated_variable() {
        let untyped_expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("x"),
            Function::Add
                .call_untyped_expr(vec![UntypedExpr::variable("y"), UntypedExpr::variable("x")]),
        ]);
        let variable_types: HashMap<&str, VarType> =
            HashMap::from([("x", VarType::U64), ("y", VarType::U64)]);
        let mut builder = CompileFnBuilder::new(&variable_types);

        let _typed_expr = builder.build_typed_expr(&untyped_expr).unwrap();
        let var_args = &builder.input_vars;

        assert_eq!(var_args.len(), 2);
        assert_eq!(var_args[0].variable_name.as_ref(), "x");
        assert_eq!(var_args[0].r#type, VarType::U64);
        assert_eq!(var_args[0].variable_id, 0);
        assert_eq!(var_args[1].variable_name.as_ref(), "y");
        assert_eq!(var_args[1].r#type, VarType::U64);
        assert_eq!(var_args[1].variable_id, 1);
    }

    #[test]
    fn test_jit_entry_returns_variable_opt_as_two_abi_values() {
        let variable_types = HashMap::new();
        let mut builder = CompileFnBuilder::new(&variable_types);
        let expression = builder
            .build_typed_expr(&UntypedExpr::literal(1u64))
            .unwrap();

        let lowered = builder.lower_typed_expr(expression).unwrap();
        let signature = &lowered.context.func.signature;

        assert_eq!(signature.params.len(), 2);
        assert!(
            signature
                .params
                .iter()
                .all(|param| param.value_type == types::I64)
        );
        assert_eq!(signature.returns.len(), 2);
        assert_eq!(signature.returns[0].value_type, types::I64);
        assert_eq!(signature.returns[1].value_type, types::I8);
    }
}
