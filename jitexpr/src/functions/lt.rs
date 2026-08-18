//! `LT` tests whether one scalar value is less than another.
//!
//! It accepts exactly two operands. Ordered production operands are strings or numbers; booleans
//! are rejected. Strings use lexicographic UTF-8 ordering. Numeric comparisons support `i64`,
//! `u64`, and `f64` combinations without converting large integers through a lossy `f64`. IEEE
//! unordered comparisons involving NaN return `false`.
//!
//! Null propagation is strict: if either operand is absent, the result is absent. This follows
//! dd-go's predicate path (`arrayCOMPARE` followed by `propagateNullsPredicateLazy`). Multivalued
//! comparison uses existential semantics in dd-go but is outside jitexpr's scalar type model.

use std::collections::HashMap;

use cranelift::frontend::FunctionBuilder;

use super::comparison::{self, OrderedComparison};
use crate::ast::{Function, InferredTypeSet, TypeError, UntypedExpr};
use crate::compile::{
    CompileError, CompileFnBuilder, LoweredValue, LoweringContext, TypedExpr, TypedExprAst,
};
use crate::functions::{FnCall, FnCallEnum};
use crate::types::VarType;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LtFnCall {
    pub(crate) args: Box<[TypedExpr]>,
}

impl FnCall for LtFnCall {
    fn infer_types<'a>(
        args: &'a [UntypedExpr],
        target_type: InferredTypeSet,
        inferred_types: &mut HashMap<&'a str, InferredTypeSet>,
    ) -> Result<InferredTypeSet, TypeError> {
        comparison::infer_types(Function::Lt, args, target_type, inferred_types)
    }

    fn call_with_types(
        args: &[UntypedExpr],
        target_type_set: InferredTypeSet,
        context: &mut CompileFnBuilder<'_, '_>,
    ) -> Result<TypedExpr, CompileError> {
        assert_eq!(args.len(), 2, "expected 2 args for LT");
        debug_assert!(target_type_set.contains(VarType::Bool));
        Ok(TypedExpr {
            return_type: VarType::Bool,
            ast: TypedExprAst::from_call(LtFnCall {
                args: comparison::apply_types(args, context)?,
            }),
        })
    }

    fn args_mut(&mut self) -> &mut [TypedExpr] {
        &mut self.args
    }

    fn emit_cranelift_ir(
        &self,
        return_type: VarType,
        context: &mut LoweringContext<'_>,
        builder: &mut FunctionBuilder<'_>,
    ) -> Result<LoweredValue, CompileError> {
        debug_assert_eq!(return_type, VarType::Bool);
        comparison::lower(&self.args, OrderedComparison::LessThan, context, builder)
    }
}

impl From<LtFnCall> for FnCallEnum {
    fn from(call: LtFnCall) -> Self {
        FnCallEnum::Lt(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{deserialize, infer_types};
    use crate::compile::compile;
    use crate::types::VariableValue;

    fn eval(expression: &str) -> Option<bool> {
        let expression = deserialize(expression).unwrap();
        let mut compiled = compile(&expression, &HashMap::new()).unwrap();
        // SAFETY: These expressions have no inputs and return nullable booleans.
        unsafe { compiled.call(&[]).as_bool() }
    }

    #[test]
    fn test_requires_two_ordered_arguments() {
        let expression = deserialize("(LT left right)").unwrap();
        let inferred_types = infer_types(&expression).unwrap();
        assert_eq!(
            inferred_types.get("left"),
            Some(&InferredTypeSet {
                string: true,
                i64: true,
                u64: true,
                f64: true,
                boolean: false,
            })
        );

        let expression = deserialize("(LT 1i64)").unwrap();
        assert!(matches!(
            infer_types(&expression),
            Err(TypeError::InvalidNumberOfArguments {
                function: Function::Lt,
                expected: 2,
                ..
            })
        ));
    }

    #[test]
    fn test_ordering_and_null_semantics() {
        assert_eq!(eval(r#"(LT "alpha" "beta")"#), Some(true));
        assert_eq!(eval("(LT -1i64 0u64)"), Some(true));
        assert_eq!(
            eval("(LT 9007199254740993u64 9007199254740992f64)"),
            Some(false)
        );
        assert_eq!(eval("(LT nanf64 0i64)"), Some(false));
        assert_eq!(eval("(LT 0i64 nanf64)"), Some(false));
        assert_eq!(eval("(LT none 1i64)"), None);
    }

    #[test]
    fn test_runtime_null_propagates() {
        let expression = deserialize("(LT left right)").unwrap();
        let variable_types = HashMap::from([("left", VarType::I64), ("right", VarType::U64)]);
        let mut compiled = compile(&expression, &variable_types).unwrap();
        // SAFETY: The input and output types match the compiled signature.
        assert_eq!(
            unsafe {
                compiled
                    .call(&[VariableValue::none(), VariableValue::some(0u64)])
                    .as_bool()
            },
            None
        );
    }
}
