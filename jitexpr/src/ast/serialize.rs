//! Serialization for [`UntypedExpr`] using a small Lisp-like syntax.
//!
//! Calls are lists whose first item is an uppercase function name, while
//! lowercase identifiers name variables. For example:
//!
//! ```text
//! (ADD 1i64 my_col)
//! ```
//!
//! Numerical literals always carry a type suffix. The other literals are
//! `none`, `true`, `false`, and quoted strings. Strings use backslash escapes.

use std::fmt;
use std::sync::Arc;

use crate::ast::{Function, Literal, UntypedExpr};

/// Serializes an untyped expression into its canonical Lisp-like form.
pub fn serialize(expr: &UntypedExpr) -> String {
    expr.to_string()
}

/// Deserializes an untyped expression from its Lisp-like form.
pub fn deserialize(input: &str) -> Result<UntypedExpr, DeserializeError> {
    Parser::new(input).parse()
}

/// An error encountered while deserializing an [`UntypedExpr`].
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("failed to deserialize expression at byte {offset}: {message}")]
pub struct DeserializeError {
    offset: usize,
    message: String,
}

impl DeserializeError {
    fn new(offset: usize, message: impl Into<String>) -> Self {
        Self {
            offset,
            message: message.into(),
        }
    }

    /// Returns the byte offset at which parsing failed.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns a description of the parsing failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for UntypedExpr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_expr(self, formatter)
    }
}

impl fmt::Debug for UntypedExpr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_expr(self, formatter)
    }
}

impl std::str::FromStr for UntypedExpr {
    type Err = DeserializeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        deserialize(input)
    }
}

fn format_expr(expr: &UntypedExpr, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match expr {
        UntypedExpr::Literal(literal) => format_literal(literal, formatter),
        UntypedExpr::Variable(variable_name) => formatter.write_str(variable_name),
        UntypedExpr::Call { function, args } => {
            write!(formatter, "({}", function_name(*function))?;
            for arg in args {
                write!(formatter, " {arg}")?;
            }
            formatter.write_str(")")
        }
    }
}

fn format_literal(literal: &Literal, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match literal {
        Literal::None => formatter.write_str("none"),
        Literal::Bool(value) => write!(formatter, "{value}"),
        Literal::U64(value) => write!(formatter, "{value}u64"),
        Literal::I64(value) => write!(formatter, "{value}i64"),
        Literal::F64(value) => write!(formatter, "{value}f64"),
        Literal::String(value) => format_string(value, formatter),
    }
}

fn format_string(value: &str, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.write_str("\"")?;
    for character in value.chars() {
        match character {
            '\"' => formatter.write_str("\\\""),
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

fn function_name(function: Function) -> &'static str {
    match function {
        Function::Abs => "ABS",
        Function::And => "AND",
        Function::Concat => "CONCAT",
        Function::Add => "ADD",
        Function::Divide => "DIVIDE",
        Function::Eq => "EQ",
        Function::Gt => "GT",
        Function::GtEq => "GT_EQ",
        Function::If => "IF",
        Function::IntMod => "INT_MOD",
        Function::Lt => "LT",
        Function::LtEq => "LT_EQ",
        Function::IsNull => "IS_NULL",
        Function::IsNotNull => "IS_NOT_NULL",
        Function::Lower => "LOWER",
        Function::Max => "MAX",
        Function::Min => "MIN",
        Function::Multiply => "MULTIPLY",
        Function::Neq => "NEQ",
        Function::Not => "NOT",
        Function::Or => "OR",
        Function::Pow => "POW",
        Function::RegexpExtract => "REGEXP_EXTRACT",
        Function::RegexpLike => "REGEXP_LIKE",
        Function::Subtract => "SUBTRACT",
        Function::Substring => "SUBSTRING",
        Function::SubstringCount => "SUBSTRING_COUNT",
        Function::TextJoin => "TEXT_JOIN",
        Function::Trim => "TRIM",
        Function::Upper => "UPPER",
    }
}

fn parse_function(name: &str, offset: usize) -> Result<Function, DeserializeError> {
    match name {
        "ABS" => Ok(Function::Abs),
        "AND" => Ok(Function::And),
        "CONCAT" => Ok(Function::Concat),
        "ADD" => Ok(Function::Add),
        "DIVIDE" => Ok(Function::Divide),
        "EQ" => Ok(Function::Eq),
        "GT" => Ok(Function::Gt),
        "GT_EQ" => Ok(Function::GtEq),
        "IF" => Ok(Function::If),
        "INT_MOD" => Ok(Function::IntMod),
        "LT" => Ok(Function::Lt),
        "LT_EQ" => Ok(Function::LtEq),
        "IS_NULL" => Ok(Function::IsNull),
        "IS_NOT_NULL" => Ok(Function::IsNotNull),
        "LOWER" => Ok(Function::Lower),
        "MAX" => Ok(Function::Max),
        "MIN" => Ok(Function::Min),
        "MULTIPLY" => Ok(Function::Multiply),
        "NEQ" => Ok(Function::Neq),
        "NOT" => Ok(Function::Not),
        "OR" => Ok(Function::Or),
        "POW" => Ok(Function::Pow),
        "REGEXP_EXTRACT" => Ok(Function::RegexpExtract),
        "REGEXP_LIKE" => Ok(Function::RegexpLike),
        "SUBTRACT" => Ok(Function::Subtract),
        "SUBSTRING" => Ok(Function::Substring),
        "SUBSTRING_COUNT" => Ok(Function::SubstringCount),
        "TEXT_JOIN" => Ok(Function::TextJoin),
        "TRIM" => Ok(Function::Trim),
        "UPPER" => Ok(Function::Upper),
        _ if !is_function_name(name) => Err(DeserializeError::new(
            offset,
            format!("function name `{name}` must be uppercase"),
        )),
        _ => Err(DeserializeError::new(
            offset,
            format!("unknown function `{name}`"),
        )),
    }
}

fn is_function_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_uppercase())
        && chars.all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
}

fn is_variable_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase())
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn parse(mut self) -> Result<UntypedExpr, DeserializeError> {
        self.skip_whitespace();
        let expr = self.parse_expr()?;
        self.skip_whitespace();
        if self.peek().is_some() {
            return Err(DeserializeError::new(
                self.offset,
                "unexpected characters after expression",
            ));
        }
        Ok(expr)
    }

    fn parse_expr(&mut self) -> Result<UntypedExpr, DeserializeError> {
        self.skip_whitespace();
        match self.peek() {
            Some('(') => self.parse_call(),
            Some('"') => self
                .parse_string()
                .map(|value| UntypedExpr::Literal(Literal::String(Arc::from(value)))),
            Some(')') => Err(DeserializeError::new(
                self.offset,
                "unexpected closing parenthesis",
            )),
            Some(_) => self.parse_atom(),
            None => Err(DeserializeError::new(self.offset, "expected an expression")),
        }
    }

    fn parse_call(&mut self) -> Result<UntypedExpr, DeserializeError> {
        let call_offset = self.offset;
        self.advance();
        self.skip_whitespace();

        if self.peek().is_none() {
            return Err(DeserializeError::new(
                call_offset,
                "unterminated function call",
            ));
        }
        if self.peek() == Some(')') {
            return Err(DeserializeError::new(
                self.offset,
                "expected a function name",
            ));
        }

        let function_offset = self.offset;
        let function_name = self.take_atom();
        if function_name.is_empty() {
            return Err(DeserializeError::new(
                function_offset,
                "expected an uppercase function name",
            ));
        }
        let function = parse_function(function_name, function_offset)?;

        let mut args = Vec::new();
        loop {
            self.skip_whitespace();
            match self.peek() {
                Some(')') => {
                    self.advance();
                    return Ok(UntypedExpr::Call { function, args });
                }
                Some(_) => args.push(self.parse_expr()?),
                None => {
                    return Err(DeserializeError::new(
                        call_offset,
                        "unterminated function call",
                    ));
                }
            }
        }
    }

    fn parse_atom(&mut self) -> Result<UntypedExpr, DeserializeError> {
        let atom_offset = self.offset;
        let atom = self.take_atom();
        match atom {
            "none" => Ok(UntypedExpr::Literal(Literal::None)),
            "true" => Ok(UntypedExpr::Literal(Literal::Bool(true))),
            "false" => Ok(UntypedExpr::Literal(Literal::Bool(false))),
            _ => self.parse_number_or_variable(atom, atom_offset),
        }
    }

    fn parse_number_or_variable(
        &self,
        atom: &str,
        atom_offset: usize,
    ) -> Result<UntypedExpr, DeserializeError> {
        if let Some(value) = atom.strip_suffix("u64")
            && let Ok(value) = value.parse::<u64>()
        {
            return Ok(UntypedExpr::Literal(Literal::U64(value)));
        }
        if let Some(value) = atom.strip_suffix("i64")
            && let Ok(value) = value.parse::<i64>()
        {
            return Ok(UntypedExpr::Literal(Literal::I64(value)));
        }
        if let Some(value) = atom.strip_suffix("f64")
            && let Ok(value) = value.parse::<f64>()
        {
            return Ok(UntypedExpr::Literal(Literal::F64(value)));
        }

        if is_variable_name(atom) {
            return Ok(UntypedExpr::Variable(Arc::from(atom)));
        }

        if is_function_name(atom) {
            return Err(DeserializeError::new(
                atom_offset,
                format!("function `{atom}` must be the first item in a list"),
            ));
        }

        Err(DeserializeError::new(
            atom_offset,
            format!("invalid literal or identifier `{atom}`"),
        ))
    }

    fn parse_string(&mut self) -> Result<String, DeserializeError> {
        let string_offset = self.offset;
        self.advance();
        let mut value = String::new();

        loop {
            let character_offset = self.offset;
            let Some(character) = self.advance() else {
                return Err(DeserializeError::new(
                    string_offset,
                    "unterminated string literal",
                ));
            };
            match character {
                '"' => return Ok(value),
                '\\' => value.push(self.parse_escape(character_offset)?),
                character if character.is_control() => {
                    return Err(DeserializeError::new(
                        character_offset,
                        "unescaped control character in string literal",
                    ));
                }
                character => value.push(character),
            }
        }
    }

    fn parse_escape(&mut self, escape_offset: usize) -> Result<char, DeserializeError> {
        let Some(escaped) = self.advance() else {
            return Err(DeserializeError::new(
                escape_offset,
                "unterminated string escape",
            ));
        };
        match escaped {
            '"' => Ok('"'),
            '\\' => Ok('\\'),
            'n' => Ok('\n'),
            'r' => Ok('\r'),
            't' => Ok('\t'),
            '0' => Ok('\0'),
            'u' => self.parse_unicode_escape(escape_offset),
            _ => Err(DeserializeError::new(
                escape_offset,
                format!("unsupported string escape `\\{escaped}`"),
            )),
        }
    }

    fn parse_unicode_escape(&mut self, escape_offset: usize) -> Result<char, DeserializeError> {
        if self.advance() != Some('{') {
            return Err(DeserializeError::new(
                escape_offset,
                "Unicode escape must start with `\\u{`",
            ));
        }

        let digits_offset = self.offset;
        while matches!(self.peek(), Some(character) if character.is_ascii_hexdigit()) {
            self.advance();
        }
        let digits = &self.input[digits_offset..self.offset];
        if digits.is_empty() || self.advance() != Some('}') {
            return Err(DeserializeError::new(
                escape_offset,
                "invalid Unicode escape",
            ));
        }

        let codepoint = u32::from_str_radix(digits, 16).ok();
        codepoint
            .and_then(char::from_u32)
            .ok_or_else(|| DeserializeError::new(escape_offset, "invalid Unicode scalar value"))
    }

    fn take_atom(&mut self) -> &'a str {
        let start = self.offset;
        while matches!(self.peek(), Some(character) if !is_delimiter(character)) {
            self.advance();
        }
        &self.input[start..self.offset]
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(character) if character.is_whitespace()) {
            self.advance();
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.offset += character.len_utf8();
        Some(character)
    }
}

fn is_delimiter(character: char) -> bool {
    character.is_whitespace() || matches!(character, '(' | ')' | '"')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_example() {
        let expr = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1i64),
            UntypedExpr::variable("my_col"),
        ]);

        assert_eq!(serialize(&expr), "(ADD 1i64 my_col)");
        assert_eq!(format!("{expr}"), "(ADD 1i64 my_col)");
        assert_eq!(format!("{expr:?}"), "(ADD 1i64 my_col)");
    }

    #[test]
    fn test_eq_round_trip() {
        let expr = Function::Eq
            .call_untyped_expr(vec![UntypedExpr::literal(1u64), UntypedExpr::literal(1i64)]);

        assert_eq!(serialize(&expr), "(EQ 1u64 1i64)");
        assert_eq!(deserialize("(EQ 1u64 1i64)").unwrap(), expr);
    }

    #[test]
    fn test_serialize_literals() {
        let cases = [
            (UntypedExpr::Literal(Literal::None), "none"),
            (UntypedExpr::literal(true), "true"),
            (UntypedExpr::literal(false), "false"),
            (UntypedExpr::literal(u64::MAX), "18446744073709551615u64"),
            (UntypedExpr::literal(i64::MIN), "-9223372036854775808i64"),
            (UntypedExpr::literal(1.5f64), "1.5f64"),
            (UntypedExpr::literal(1.0f64), "1f64"),
        ];

        for (expr, expected) in cases {
            assert_eq!(serialize(&expr), expected);
            assert_eq!(deserialize(expected).unwrap(), expr);
        }
    }

    #[test]
    fn test_nested_call_and_escaped_string_round_trip() {
        let string = "quoted: \"hello\"\\world\n\t\0\u{7} café";
        let regexp_extract = Function::RegexpExtract.call_untyped_expr(vec![
            UntypedExpr::variable("message"),
            UntypedExpr::literal(string),
            UntypedExpr::literal(1u64),
        ]);
        let expr =
            Function::Add.call_untyped_expr(vec![regexp_extract, UntypedExpr::literal(2i64)]);

        let serialized = serialize(&expr);
        assert_eq!(
            serialized,
            "(ADD (REGEXP_EXTRACT message \"quoted: \\\"hello\\\"\\\\world\\n\\t\\0\\u{7} café\" \
             1u64) 2i64)"
        );
        assert_eq!(deserialize(&serialized).unwrap(), expr);
    }

    #[test]
    fn test_deserialize_accepts_whitespace() {
        let parsed = deserialize(" \n ( ADD\t1i64\nmy_col ) \r").unwrap();
        let expected = Function::Add.call_untyped_expr(vec![
            UntypedExpr::literal(1i64),
            UntypedExpr::variable("my_col"),
        ]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_float_special_values_round_trip() {
        for value in [f64::INFINITY, f64::NEG_INFINITY, -0.0] {
            let serialized = serialize(&UntypedExpr::literal(value));
            let UntypedExpr::Literal(Literal::F64(parsed)) = deserialize(&serialized).unwrap()
            else {
                panic!("expected an f64 literal");
            };
            assert_eq!(parsed.to_bits(), value.to_bits());
        }

        let serialized = serialize(&UntypedExpr::literal(f64::NAN));
        let UntypedExpr::Literal(Literal::F64(parsed)) = deserialize(&serialized).unwrap() else {
            panic!("expected an f64 literal");
        };
        assert!(parsed.is_nan());
    }

    #[test]
    fn test_from_str() {
        let parsed: UntypedExpr = "(ADD 3u64 value)".parse().unwrap();
        assert_eq!(serialize(&parsed), "(ADD 3u64 value)");
    }

    #[test]
    fn test_deserialize_errors() {
        let cases = [
            ("", 0, "expected an expression"),
            ("()", 1, "expected a function name"),
            ("(add 1i64)", 1, "must be uppercase"),
            ("(UNKNOWN 1i64)", 1, "unknown function"),
            ("ADD", 0, "must be the first item in a list"),
            ("1i32", 0, "invalid literal or identifier"),
            ("\"unterminated", 0, "unterminated string literal"),
            ("\"bad\\x\"", 4, "unsupported string escape"),
            ("(ADD 1i64", 0, "unterminated function call"),
            ("value other", 6, "unexpected characters after expression"),
        ];

        for (input, offset, expected_message) in cases {
            let error = deserialize(input).unwrap_err();
            assert_eq!(error.offset(), offset, "input: {input}");
            assert!(
                error.message().contains(expected_message),
                "input: {input}; error: {error}"
            );
        }
    }
}
