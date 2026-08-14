use std::collections::HashMap;
use std::error::Error;

use jitexpr::ast::{Function, InferredTypeSet, UntypedExpr, apply_types, infer_types};
use jitexpr::compile::{CompiledFunction, compile};
use jitexpr::types::VarType;

fn main() -> Result<(), Box<dyn Error>> {
    // A simple expression that goes:
    // my_column + 1
    let untyped_expr = Function::Add.call_untyped_expr(vec![
        UntypedExpr::variable("my_col"),
        UntypedExpr::literal(1.0f64),
    ]);

    let inferred_types = infer_types(&untyped_expr)?;
    assert_eq!(
        inferred_types.get("my_col").unwrap(),
        &InferredTypeSet::NUMERICAL
    );

    let variable_types: HashMap<&str, VarType> =
        std::iter::once(("my_col", VarType::F64)).collect();

    let compiled_fn: CompiledFunction = compile(&untyped_expr, &variable_types).unwrap();

    // let function = compile(&expression, selected_types)?;

    // assert_eq!(evaluate(&function, 100, 4), 25.0);
    // assert_eq!(evaluate(&function, 100, 0), 0.0);
    Ok(())
}

// fn evaluate(function: &CompiledFunction, request_size: u64, elapsed: u64) -> f64 {
//     // NamedInput defines the positional order expected by the generated code.
//     let args = function
//         .inputs()
//         .iter()
//         .map(|input| {
//             assert_eq!(input.var_type(), VarType::U64);
//             match input.name() {
//                 "request_size" => Variable::from_u64(request_size),
//                 "elapsed" => Variable::from_u64(elapsed),
//                 name => panic!("unexpected input `{name}`"),
//             }
//         })
//         .collect::<Vec<_>>();

//     let mut result = Variable::null();
//     // SAFETY: `args` follows `function.inputs()` and every payload matches its
//     // reported VarType. `result` is a writable Variable slot.
//     unsafe { function.call(&args, &mut result) };
//     assert!(!result.is_null());
//     // SAFETY: The compiled signature reports F64 and the result is non-null.
//     unsafe { result.as_f64() }
// }

// use jitexpr::ast::Expr;

// fn main() -> Result<(), Box<dyn Error>> {
//     // COALESCE(request_size / elapsed, 0.0). Division produces F64 and returns
//     // null when its divisor is zero, so COALESCE supplies the fallback.
//     let expression = Expr::call(
//         Function::Coalesce,
//         [
//             Expr::call(
//                 Function::Divide,
//                 [Expr::variable("request_size"), Expr::variable("elapsed")],
//             ),
//             Literal::F64(0.0).into(),
//         ],
//     );

//     let argument_names = expression.list_argument_names();
//     println!("referenced fields: {argument_names:?}");

//     // An integrating crate would obtain these entries by looking up the
//     // referenced fields in Tantivy's columnar schema.
//     let available_types = HashMap::from([
//         (
//             "request_size".to_string(),
//             AvailableVarTypes {
//                 numerical: Some(NumericalType::U64),
//                 boolean: false,
//                 string: false,
//             },
//         ),
//         (
//             "elapsed".to_string(),
//             AvailableVarTypes {
//                 numerical: Some(NumericalType::U64),
//                 boolean: false,
//                 string: false,
//             },
//         ),
//     ]);

//     let selected_types = infer_types(&expression, &available_types)?;
//     let function = compile(&expression, selected_types)?;

//     assert_eq!(evaluate(&function, 100, 4), 25.0);
//     assert_eq!(evaluate(&function, 100, 0), 0.0);
//     Ok(())
// }

// fn evaluate(function: &CompiledFunction, request_size: u64, elapsed: u64) -> f64 {
//     // NamedInput defines the positional order expected by the generated code.
//     let args = function
//         .inputs()
//         .iter()
//         .map(|input| {
//             assert_eq!(input.var_type(), VarType::U64);
//             match input.name() {
//                 "request_size" => Variable::from_u64(request_size),
//                 "elapsed" => Variable::from_u64(elapsed),
//                 name => panic!("unexpected input `{name}`"),
//             }
//         })
//         .collect::<Vec<_>>();

//     let mut result = Variable::null();
//     // SAFETY: `args` follows `function.inputs()` and every payload matches its
//     // reported VarType. `result` is a writable Variable slot.
//     unsafe { function.call(&args, &mut result) };
//     assert!(!result.is_null());
//     // SAFETY: The compiled signature reports F64 and the result is non-null.
//     unsafe { result.as_f64() }
// }
