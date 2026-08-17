use std::collections::HashMap;
use std::error::Error;

use jitexpr::ast::{Function, InferredTypeSet, UntypedExpr, infer_types};
use jitexpr::compile::{CompiledFn, compile};
use jitexpr::types::{VarType, VariableOpt};

fn main() -> Result<(), Box<dyn Error>> {
    // A simple expression that goes:
    // my_col + 1
    let untyped_expr = Function::Add.call_untyped_expr(vec![
        UntypedExpr::variable("my_col"),
        UntypedExpr::literal(1.0f64),
    ]);

    // Infer types does not return specific types, but instead a set of acceptable
    // types for each variables.
    let inferred_types: HashMap<&str, InferredTypeSet> = infer_types(&untyped_expr)?;
    assert_eq!(
        inferred_types.get("my_col").unwrap(),
        &InferredTypeSet::NUMERICAL
    );

    // This is then up to us to decide the actual type for each variable.
    // In tantivy, this means picking the first column with a type in inferred_types.
    //
    // If none match then we should use the VarType::None.
    let variable_types: HashMap<&str, VarType> =
        std::iter::once(("my_col", VarType::F64)).collect();

    let compiled_fn: CompiledFn = compile(&untyped_expr, &variable_types)?;

    // We use a nullable wrapper around the value union to pass typed variables.
    // For present values, it is up to us to populate the correct union member.
    // Not doing so is UB.
    let input: Box<[VariableOpt]> = vec![VariableOpt::from(1.2f64)].into_boxed_slice();
    // The initialization does not really matter.
    let mut output = VariableOpt::default();
    unsafe { compiled_fn.call(&input[..], &mut output) };
    assert_eq!(unsafe { output.as_f64() }, Some(1.2f64 + 1.0f64));

    Ok(())
}
