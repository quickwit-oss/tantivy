//! Prints typed expressions and native assembly for serialized `UntypedExpr` values read from
//! stdin.
//!
//! The serialization does not attach concrete types to variables. This tool
//! therefore uses `Str` for string variables, `Bool` for boolean variables,
//! and `F64` for numerical or otherwise unconstrained variables.
//! ANSI colors are enabled when stdout is a terminal and can be disabled with `NO_COLOR`.

use std::collections::HashMap;
use std::io::{self, BufRead, IsTerminal, Write};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use jitexpr::ast::{
    DeserializeError, InferredTypeSet, TypeError, deserialize, infer_types,
    serialize as serialize_untyped,
};
use jitexpr::compile::{CompileError, compile, compile_to_assembly, serialize as serialize_typed};
use jitexpr::types::VarType;

const RESET: &str = "\x1b[0m";
const BOLD_CYAN: &str = "\x1b[1;36m";
const BOLD_MAGENTA: &str = "\x1b[1;35m";
const BLUE: &str = "\x1b[34m";
const CYAN: &str = "\x1b[36m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const DIM: &str = "\x1b[2m";

#[derive(Debug, thiserror::Error)]
enum ExpressionError {
    #[error(transparent)]
    Deserialize(#[from] DeserializeError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error(transparent)]
    Compile(#[from] CompileError),
}

struct LineOutput {
    input_expression: String,
    typed_expression: String,
    assembly: String,
    codegen_duration: Duration,
}

fn main() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();
    let color = stdout.is_terminal() && std::env::var_os("NO_COLOR").is_none();
    match process_lines(stdin.lock(), stdout.lock(), stderr.lock(), color) {
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
    color: bool,
) -> io::Result<bool> {
    let mut all_succeeded = true;
    let mut wrote_output = false;

    for (line_index, line) in input.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        match compile_line(&line) {
            Ok(line_output) => {
                if wrote_output {
                    writeln!(output)?;
                    write_styled(&mut output, DIM, &"-".repeat(80), color)?;
                    writeln!(output, "\n")?;
                }
                write_header(&mut output, "Input expression", color)?;
                write_expression(&mut output, &line_output.input_expression, color)?;
                writeln!(output, "\n")?;

                write_header(&mut output, "Typed expression", color)?;
                write_expression(&mut output, &line_output.typed_expression, color)?;
                writeln!(output, "\n")?;

                write_styled(&mut output, BOLD_CYAN, "Code generation time:", color)?;
                write!(output, " ")?;
                write_styled(
                    &mut output,
                    YELLOW,
                    &format!("{:?}", line_output.codegen_duration),
                    color,
                )?;
                writeln!(output, "\n")?;

                write_header(&mut output, "Assembly", color)?;
                write_assembly(&mut output, &line_output.assembly, color)?;
                wrote_output = true;
            }
            Err(error) => {
                writeln!(errors, "line {}: {error}", line_index + 1)?;
                all_succeeded = false;
            }
        }
    }

    Ok(all_succeeded)
}

fn write_header(output: &mut impl Write, header: &str, color: bool) -> io::Result<()> {
    write_styled(output, BOLD_CYAN, header, color)?;
    writeln!(output, ":")
}

fn write_styled(output: &mut impl Write, style: &str, text: &str, color: bool) -> io::Result<()> {
    if color {
        write!(output, "{style}{text}{RESET}")
    } else {
        output.write_all(text.as_bytes())
    }
}

fn write_expression(output: &mut impl Write, expression: &str, color: bool) -> io::Result<()> {
    if !color {
        return output.write_all(expression.as_bytes());
    }

    let mut offset = 0;
    while offset < expression.len() {
        let character = expression[offset..]
            .chars()
            .next()
            .expect("offset is before end of expression");
        if character == '"' {
            let end = quoted_literal_end(expression, offset);
            write_styled(output, GREEN, &expression[offset..end], true)?;
            offset = end;
        } else if character.is_whitespace() {
            write!(output, "{character}")?;
            offset += character.len_utf8();
        } else if matches!(character, '(' | ')' | '[' | ']' | ':') {
            write_styled(output, DIM, &expression[offset..offset + 1], true)?;
            offset += 1;
        } else {
            let end = expression[offset..]
                .char_indices()
                .find_map(|(relative_offset, character)| {
                    (relative_offset > 0
                        && (character.is_whitespace()
                            || matches!(character, '(' | ')' | '[' | ']' | ':' | '"')))
                    .then_some(offset + relative_offset)
                })
                .unwrap_or(expression.len());
            let token = &expression[offset..end];
            let style = expression_token_style(token, &expression[end..]);
            write_styled(output, style, token, true)?;
            offset = end;
        }
    }
    Ok(())
}

fn quoted_literal_end(expression: &str, quote_offset: usize) -> usize {
    let mut escaped = false;
    for (relative_offset, character) in expression[quote_offset + 1..].char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return quote_offset + 1 + relative_offset + 1;
        }
    }
    expression.len()
}

fn expression_token_style(token: &str, suffix: &str) -> &'static str {
    if suffix.starts_with(':')
        && matches!(
            token,
            "boolean" | "float64" | "uint64" | "int64" | "string" | "none"
        )
    {
        CYAN
    } else if token
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_uppercase())
        && token.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
    {
        BOLD_MAGENTA
    } else if matches!(token, "true" | "false" | "none")
        || token.ends_with("i64")
        || token.ends_with("u64")
        || token.ends_with("f64")
    {
        YELLOW
    } else {
        BLUE
    }
}

fn write_assembly(output: &mut impl Write, assembly: &str, color: bool) -> io::Result<()> {
    if !color {
        output.write_all(assembly.as_bytes())?;
        if !assembly.ends_with('\n') {
            writeln!(output)?;
        }
        return Ok(());
    }

    for line in assembly.lines() {
        let instruction = line.trim_start();
        let indentation = &line[..line.len() - instruction.len()];
        output.write_all(indentation.as_bytes())?;
        if instruction.starts_with("block") && instruction.ends_with(':') {
            write_styled(output, BOLD_MAGENTA, instruction, true)?;
        } else if let Some(opcode_end) = instruction.find(char::is_whitespace) {
            write_styled(output, CYAN, &instruction[..opcode_end], true)?;
            output.write_all(&instruction.as_bytes()[opcode_end..])?;
        } else {
            write_styled(output, CYAN, instruction, true)?;
        }
        writeln!(output)?;
    }
    Ok(())
}

fn compile_line(line: &str) -> Result<LineOutput, ExpressionError> {
    let expression = deserialize(line)?;
    let inferred_types = infer_types(&expression)?;
    let variable_types = inferred_types
        .into_iter()
        .map(|(name, inferred_type)| (name, concrete_type(inferred_type)))
        .collect::<HashMap<_, _>>();

    let codegen_start = Instant::now();
    let compiled_fn = compile(&expression, &variable_types)?;
    let codegen_duration = codegen_start.elapsed();
    drop(compiled_fn);

    let input_expression = serialize_untyped(&expression);
    let typed_expression = serialize_typed(&expression, &variable_types)?;
    let assembly = compile_to_assembly(&expression, &variable_types)?;
    Ok(LineOutput {
        input_expression,
        typed_expression,
        assembly,
        codegen_duration,
    })
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
        let line_output = compile_line("(ADD 1i64 my_col)").unwrap();

        assert_eq!(line_output.input_expression, "(ADD 1i64 my_col)");
        assert_eq!(
            line_output.typed_expression,
            "[int64: ADD 1i64 [int64: my_col]]"
        );
        assert!(line_output.assembly.contains("block0:"));
        assert!(!line_output.assembly.trim().is_empty());
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

        let all_succeeded = process_lines(input, &mut output, &mut errors, false).unwrap();

        assert!(!all_succeeded);
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.matches("Code generation time:").count(), 2);
        assert_eq!(output.matches("Typed expression:").count(), 2);
        assert_eq!(output.matches("Input expression:").count(), 2);
        assert_eq!(output.matches(&"-".repeat(80)).count(), 1);
        assert_eq!(output.matches("block0:").count(), 2);
        assert!(!output.contains("\x1b["));
        assert!(String::from_utf8(errors).unwrap().contains("line 2:"));
    }

    #[test]
    fn test_process_lines_ignores_empty_lines() {
        let mut output = Vec::new();
        let mut errors = Vec::new();

        let all_succeeded =
            process_lines(b" \n\t\n".as_slice(), &mut output, &mut errors, false).unwrap();

        assert!(all_succeeded);
        assert!(output.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_process_lines_colors_terminal_output() {
        let mut output = Vec::new();
        let mut errors = Vec::new();

        let all_succeeded = process_lines(
            b"(ADD 1i64 my_col)\n".as_slice(),
            &mut output,
            &mut errors,
            true,
        )
        .unwrap();

        assert!(all_succeeded);
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("\x1b[1;36mInput expression\x1b[0m:"));
        assert!(output.contains("\x1b[1;35mADD\x1b[0m"));
        assert!(output.contains("\x1b[36mint64\x1b[0m"));
        assert!(output.contains("\x1b[33m1i64\x1b[0m"));
        assert!(errors.is_empty());
    }
}
