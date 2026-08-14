//! Prints native assembly for serialized `UntypedExpr` values read from stdin.
//!
//! The serialization does not attach concrete types to variables. This tool
//! therefore uses `Str` for string variables, `Bool` for boolean variables,
//! and `F64` for numerical or otherwise unconstrained variables.

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use jitexpr::ast::{DeserializeError, InferredTypeSet, TypeError, deserialize, infer_types};
use jitexpr::compile::{CompileError, compile_to_assembly};
use jitexpr::types::VarType;

#[derive(Debug, thiserror::Error)]
enum ExpressionError {
    #[error(transparent)]
    Deserialize(#[from] DeserializeError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error(transparent)]
    Compile(#[from] CompileError),
}

fn main() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();
    match process_lines(stdin.lock(), stdout.lock(), stderr.lock()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("I/O error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Returns whether every non-empty input line compiled successfully.
fn process_lines(
    input: impl BufRead,
    mut output: impl Write,
    mut errors: impl Write,
) -> io::Result<bool> {
    let mut all_succeeded = true;
    let mut wrote_assembly = false;

    for (line_index, line) in input.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        match compile_line(&line) {
            Ok(assembly) => {
                if wrote_assembly {
                    writeln!(output)?;
                }
                output.write_all(assembly.as_bytes())?;
                if !assembly.ends_with('\n') {
                    writeln!(output)?;
                }
                wrote_assembly = true;
            }
            Err(error) => {
                writeln!(errors, "line {}: {error}", line_index + 1)?;
                all_succeeded = false;
            }
        }
    }

    Ok(all_succeeded)
}

fn compile_line(line: &str) -> Result<String, ExpressionError> {
    let expression = deserialize(line)?;
    let inferred_types = infer_types(&expression)?;
    let variable_types = inferred_types
        .into_iter()
        .map(|(name, inferred_type)| (name, concrete_type(inferred_type)))
        .collect::<HashMap<_, _>>();
    Ok(compile_to_assembly(&expression, &variable_types)?)
}

fn concrete_type(inferred_type: InferredTypeSet) -> VarType {
    if inferred_type == InferredTypeSet::STRING {
        VarType::Str
    } else if inferred_type == InferredTypeSet::BOOLEAN {
        VarType::Bool
    } else if inferred_type.i64 {
        VarType::I64
    } else if inferred_type.u64 {
        VarType::U64
    } else if inferred_type.f64 {
        VarType::F64
    } else if inferred_type.string {
        VarType::Str
    } else if inferred_type.boolean {
        VarType::Bool
    } else {
        VarType::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_line_infers_variable_type() {
        let assembly = compile_line("(ADD 1i64 my_col)").unwrap();

        assert!(assembly.contains("block0:"));
        assert!(!assembly.trim().is_empty());
    }

    #[test]
    fn test_concrete_type_selects_an_inferred_numeric_type() {
        assert_eq!(concrete_type(InferredTypeSet::ALL), VarType::I64);
        assert_eq!(concrete_type(InferredTypeSet::I64), VarType::I64);
        assert_eq!(concrete_type(InferredTypeSet::U64), VarType::U64);
        assert_eq!(concrete_type(InferredTypeSet::F64), VarType::F64);
        assert_eq!(concrete_type(InferredTypeSet::NONE), VarType::None);
    }

    #[test]
    fn test_process_lines_continues_after_an_error() {
        let input = b"1i64\nnot-valid!\n2u64\n".as_slice();
        let mut output = Vec::new();
        let mut errors = Vec::new();

        let all_succeeded = process_lines(input, &mut output, &mut errors).unwrap();

        assert!(!all_succeeded);
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.matches("block0:").count(), 2);
        assert!(String::from_utf8(errors).unwrap().contains("line 2:"));
    }

    #[test]
    fn test_process_lines_ignores_empty_lines() {
        let mut output = Vec::new();
        let mut errors = Vec::new();

        let all_succeeded = process_lines(b" \n\t\n".as_slice(), &mut output, &mut errors).unwrap();

        assert!(all_succeeded);
        assert!(output.is_empty());
        assert!(errors.is_empty());
    }
}
