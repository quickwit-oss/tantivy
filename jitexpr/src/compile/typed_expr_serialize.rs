//! Serialization for the normalized typed expression tree.
//!
//! Calls, variables, and coercions use `[type: expression]`, while literals retain the canonical
//! untyped literal syntax because numerical suffixes and literal spellings already identify their
//! types. For example:
//!
//! ```text
//! [int64: ADD 1i64 [int64: my_col]]
//! ```

use std::fmt;

use super::{TypedExpr, TypedExprAst, TypedLiteral};
use crate::types::VarType;

pub(super) fn serialize(expr: &TypedExpr) -> String {
    expr.to_string()
}

impl fmt::Display for TypedExpr {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        format_expr(self, formatter)
    }
}

fn format_expr(expr: &TypedExpr, formatter: &mut fmt::Formatter) -> fmt::Result {
    if let TypedExprAst::Literal(literal) = &expr.ast {
        return format_literal(literal, formatter);
    }

    write!(formatter, "[{}: ", type_name(expr.return_type))?;
    match &expr.ast {
        TypedExprAst::Literal(_) => unreachable!(),
        TypedExprAst::Variable(variable) => formatter.write_str(&variable.variable_name)?,
        TypedExprAst::Coerce { expr, .. } => write!(formatter, "COERCE {expr}")?,
        TypedExprAst::FnCall(fn_call) => fn_call.serialize(formatter)?,
    }
    formatter.write_str("]")
}

fn format_literal(literal: &TypedLiteral, formatter: &mut fmt::Formatter) -> fmt::Result {
    match literal {
        TypedLiteral::None => formatter.write_str("none"),
        TypedLiteral::Bool(value) => write!(formatter, "{value}"),
        TypedLiteral::U64(value) => write!(formatter, "{value}u64"),
        TypedLiteral::I64(value) => write!(formatter, "{value}i64"),
        TypedLiteral::F64(value) => write!(formatter, "{value}f64"),
        TypedLiteral::String(value) => format_string_literal(value, formatter),
    }
}

pub(crate) fn format_function_call<'a>(
    name: &str,
    args: impl IntoIterator<Item = &'a TypedExpr>,
    formatter: &mut fmt::Formatter,
) -> fmt::Result {
    formatter.write_str(name)?;
    for arg in args {
        write!(formatter, " {arg}")?;
    }
    Ok(())
}

pub(crate) fn format_string_literal(value: &str, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("\"")?;
    for character in value.chars() {
        match character {
            '"' => formatter.write_str("\\\""),
            '\\' => formatter.write_str("\\\\"),
            '\n' => formatter.write_str("\\n"),
            '\r' => formatter.write_str("\\r"),
            '\t' => formatter.write_str("\\t"),
            '\0' => formatter.write_str("\\0"),
            character if character.is_control() => {
                write!(formatter, "{}", character.escape_unicode())
            }
            character => write!(formatter, "{character}"),
        }?;
    }
    formatter.write_str("\"")
}

fn type_name(var_type: VarType) -> &'static str {
    match var_type {
        VarType::Bool => "boolean",
        VarType::F64 => "float64",
        VarType::U64 => "uint64",
        VarType::I64 => "int64",
        VarType::Str => "string",
        VarType::None => "none",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::types::VarType;
    use crate::{ast, compile};

    fn serialize(expression: &str, variable_types: &HashMap<&str, VarType>) -> String {
        let expression = ast::deserialize(expression).unwrap();
        compile::serialize(&expression, variable_types).unwrap()
    }

    #[test]
    fn test_serializes_typed_add_expression() {
        let variable_types = HashMap::from([("my_col", VarType::I64)]);

        assert_eq!(
            serialize("(ADD 1i64 my_col)", &variable_types),
            "[int64: ADD 1i64 [int64: my_col]]"
        );
    }

    #[test]
    fn test_serializes_explicit_coercion() {
        let variable_types = HashMap::from([("my_col", VarType::I64)]);

        assert_eq!(
            serialize("(ADD 1.5f64 my_col)", &variable_types),
            "[float64: ADD 1.5f64 [float64: COERCE [int64: my_col]]]"
        );
    }

    #[test]
    fn test_serializes_all_variable_types_and_literals() {
        let cases = [
            (VarType::Bool, "[boolean: value]"),
            (VarType::F64, "[float64: value]"),
            (VarType::U64, "[uint64: value]"),
            (VarType::I64, "[int64: value]"),
            (VarType::Str, "[string: value]"),
        ];
        for (var_type, expected) in cases {
            assert_eq!(
                serialize("value", &HashMap::from([("value", var_type)])),
                expected
            );
        }

        let literals = [
            ("none", "none"),
            ("true", "true"),
            ("1u64", "1i64"),
            ("18446744073709551615u64", "18446744073709551615u64"),
            ("-2i64", "-2i64"),
            ("1.5f64", "1.5f64"),
            (
                r#""quoted: \"hello\"\\world\n\t\0\u{7} café""#,
                r#""quoted: \"hello\"\\world\n\t\0\u{7} café""#,
            ),
        ];
        for (literal, expected) in literals {
            assert_eq!(serialize(literal, &HashMap::new()), expected);
        }
    }

    #[test]
    fn test_serializes_normalized_compile_time_arguments() {
        let variable_types = HashMap::from([
            ("message", VarType::Str),
            ("number", VarType::F64),
            ("other", VarType::Str),
        ]);
        let cases = [
            (
                "(CONCAT \" / \" \"TRUE\" message other)",
                "[string: CONCAT \" / \" \"true\" [string: message] [string: other]]",
            ),
            (
                "(LEFT message 2i64)",
                "[string: LEFT [string: message] 2u64]",
            ),
            (
                r#"(REGEXP_EXTRACT message "([a-z]+)")"#,
                r#"[string: REGEXP_EXTRACT [string: message] "([a-z]+)" 0u64]"#,
            ),
            (
                r#"(REGEXP_LIKE message "[a-z]+")"#,
                r#"[boolean: REGEXP_LIKE [string: message] "[a-z]+"]"#,
            ),
            (
                "(RIGHT message 2i64)",
                "[string: RIGHT [string: message] 2u64]",
            ),
            ("(ROUND number)", "[int64: ROUND [float64: number] 0i64]"),
            (
                "(SPLIT_AFTER message \".\")",
                "[string: SPLIT_AFTER [string: message] \".\" 0u64]",
            ),
            (
                "(SPLIT_BEFORE message \".\")",
                "[string: SPLIT_BEFORE [string: message] \".\" 0u64]",
            ),
            (
                "(SUBSTRING message 1i64 2i64)",
                "[string: SUBSTRING [string: message] 1u64 2u64]",
            ),
            (
                "(TEXT_JOIN \" / \" \"FALSE\" message other)",
                "[string: TEXT_JOIN \" / \" \"false\" [string: message] [string: other]]",
            ),
            (
                "(TRIM message \"x\" \"BOTH\")",
                "[string: TRIM [string: message] \"x\" \"both\"]",
            ),
        ];

        for (expression, expected) in cases {
            assert_eq!(serialize(expression, &variable_types), expected);
        }
    }
}
