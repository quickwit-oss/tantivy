//! Serialization for [`UntypedExpr`] using a small Lisp-like syntax.
//!
//! Calls are lists whose first item is a recognized uppercase function name.
//! Elsewhere, atoms name variables unless they match a literal. For example:
//!
//! ```text
//! (ADD 1i64 my_col)
//! ```
//!
//! Numerical literals always carry a type suffix. Parsing rejects non-finite
//! `f64` literals (NaN, infinities, and overflow). The other literals are
//! `none`, `true`, `false`, and double-quoted strings. Backticks quote variable
//! names containing whitespace or syntax characters, or matching literals:
//!
//! ```text
//! (ADD `1u64` 1u64)
//! ```
//!
//! Quoted variables use the same backslash escapes as strings, plus `` \` `` for
//! a literal backtick. Serialization quotes names only when needed for an
//! unambiguous round trip. Variable names are not restricted to ASCII or checked
//! against a schema; field-name validation remains the caller's responsibility.

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
        UntypedExpr::Variable(variable_name) => {
            if can_format_bare_variable(variable_name) {
                // This is not ambiguous with a literal, let's write it without quotation marks.
                return formatter.write_str(variable_name);
            }
            format_quoted(variable_name, '`', formatter)
        }
        UntypedExpr::Call { function, args } => {
            write!(formatter, "({}", function_name(*function))?;
            for arg in args {
                write!(formatter, " {arg}")?;
            }
            formatter.write_str(")")
        }
    }
}

fn format_literal(literal: &Literal, formatter: &mut fmt::Formatter) -> fmt::Result {
    match literal {
        Literal::None => formatter.write_str("none"),
        Literal::Bool(value) => write!(formatter, "{value}"),
        Literal::U64(value) => write!(formatter, "{value}u64"),
        Literal::I64(value) => write!(formatter, "{value}i64"),
        Literal::F64(value) => write!(formatter, "{value}f64"),
        Literal::String(value) => format_quoted(value, '"', formatter),
    }
}

fn format_quoted(value: &str, quote: char, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(formatter, "{quote}")?;
    for character in value.chars() {
        match character {
            character if character == quote => write!(formatter, "\\{character}"),
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
    write!(formatter, "{quote}")
}

fn function_name(function: Function) -> &'static str {
    match function {
        Function::Add => "ADD",
        Function::IsNull => "IS_NULL",
        Function::RegexpExtract => "REGEXP_EXTRACT",
    }
}

fn parse_function(name: &str, offset: usize) -> Result<Function, DeserializeError> {
    match name {
        "ADD" => Ok(Function::Add),
        "IS_NULL" => Ok(Function::IsNull),
        "REGEXP_EXTRACT" => Ok(Function::RegexpExtract),
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

fn can_format_bare_variable(name: &str) -> bool {
    // Quote all recognized literal forms, including non-finite floats that the
    // expression parser rejects, so variable names always round-trip.
    !name.is_empty()
        && name
            .chars()
            .all(|c: char| c.is_ascii_alphabetic() || c == '_' || c == '.')
        && parse_literal_atom(name).is_none()
}

// Recognition intentionally includes non-finite floats: parse_atom must reject
// them rather than fall back to variables, and serialization must quote those names.
fn parse_literal_atom(atom: &str) -> Option<Literal> {
    match atom {
        "none" => return Some(Literal::None),
        "true" => return Some(Literal::Bool(true)),
        "false" => return Some(Literal::Bool(false)),
        _ => {}
    }
    if let Some(value_str) = atom.strip_suffix("u64") {
        let val = value_str.parse::<u64>().ok()?;
        return Some(Literal::U64(val));
    }
    if let Some(value_str) = atom.strip_suffix("i64") {
        let val = value_str.parse::<i64>().ok()?;
        return Some(Literal::I64(val));
    }
    if let Some(value_str) = atom.strip_suffix("f64") {
        let val = value_str.parse::<f64>().ok()?;
        return Some(Literal::F64(val));
    }
    None
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
                .parse_quoted('"', "string literal")
                .map(|value| UntypedExpr::Literal(Literal::String(Arc::from(value)))),
            Some('`') => self
                .parse_quoted('`', "quoted variable")
                .map(|value| UntypedExpr::Variable(Arc::from(value))),
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
        if let Some(literal) = parse_literal_atom(atom) {
            if let Literal::F64(value) = &literal
                && !value.is_finite()
            {
                return Err(DeserializeError::new(
                    atom_offset,
                    format!("f64 literal `{atom}` must be finite"),
                ));
            }
            return Ok(UntypedExpr::Literal(literal));
        }
        Ok(UntypedExpr::Variable(Arc::from(atom)))
    }

    fn parse_quoted(&mut self, quote: char, kind: &str) -> Result<String, DeserializeError> {
        let quoted_offset = self.offset;
        self.advance();
        let mut value = String::new();

        loop {
            let character_offset = self.offset;
            let Some(character) = self.advance() else {
                return Err(DeserializeError::new(
                    quoted_offset,
                    format!("unterminated {kind}"),
                ));
            };
            match character {
                character if character == quote => return Ok(value),
                '\\' => value.push(self.parse_escape(character_offset, quote)?),
                character if character.is_control() => {
                    return Err(DeserializeError::new(
                        character_offset,
                        format!("unescaped control character in {kind}"),
                    ));
                }
                character => value.push(character),
            }
        }
    }

    fn parse_escape(
        &mut self,
        escape_offset: usize,
        quote: char,
    ) -> Result<char, DeserializeError> {
        let Some(escaped) = self.advance() else {
            return Err(DeserializeError::new(
                escape_offset,
                "unterminated string escape",
            ));
        };
        match escaped {
            '`' if quote == '`' => Ok('`'),
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
    character.is_whitespace() || matches!(character, '(' | ')' | '"' | '`')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_example() {
        let expr = UntypedExpr::call(
            Function::Add,
            vec![UntypedExpr::literal(1i64), UntypedExpr::variable("my_col")],
        )
        .unwrap();

        assert_eq!(serialize(&expr), "(ADD 1i64 my_col)");
        assert_eq!(format!("{expr}"), "(ADD 1i64 my_col)");
        assert_eq!(format!("{expr:?}"), "(ADD 1i64 my_col)");
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
        let regexp_extract = UntypedExpr::call(
            Function::RegexpExtract,
            vec![
                UntypedExpr::variable("message"),
                UntypedExpr::literal(string),
                UntypedExpr::literal(1u64),
            ],
        )
        .unwrap();
        let expr = UntypedExpr::call(
            Function::Add,
            vec![regexp_extract, UntypedExpr::literal(2i64)],
        )
        .unwrap();

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
        let expected = UntypedExpr::call(
            Function::Add,
            vec![UntypedExpr::literal(1i64), UntypedExpr::variable("my_col")],
        )
        .unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_finite_float_edge_values_round_trip() {
        for value in [
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            -0.0,
        ] {
            let serialized = serialize(&UntypedExpr::literal(value));
            let UntypedExpr::Literal(Literal::F64(parsed)) = deserialize(&serialized).unwrap()
            else {
                panic!("expected an f64 literal");
            };
            assert_eq!(parsed.to_bits(), value.to_bits());
        }
    }

    #[test]
    fn test_non_finite_float_literals_are_rejected() {
        for atom in [
            "NaNf64",
            "nanf64",
            "+NaNf64",
            "-NaNf64",
            "inff64",
            "+inff64",
            "-inff64",
            "infinityf64",
            "-INFINITYf64",
            "1e999f64",
            "-1e999f64",
        ] {
            for (input, offset) in [(atom.to_string(), 0), (format!("(ADD 1u64 {atom})"), 10)] {
                let error = deserialize(&input).unwrap_err();
                assert_eq!(error.offset(), offset, "input: {input}");
                assert_eq!(
                    error.message(),
                    format!("f64 literal `{atom}` must be finite")
                );
            }

            // Rejected literal spellings are still usable as quoted field names.
            let variable = UntypedExpr::variable(atom);
            let serialized = serialize(&variable);
            assert_eq!(serialized, format!("`{atom}`"));
            assert_eq!(deserialize(&serialized).unwrap(), variable);
        }
    }

    #[test]
    fn test_from_str() {
        let parsed: UntypedExpr = "(ADD 3u64 value)".parse().unwrap();
        assert_eq!(serialize(&parsed), "(ADD 3u64 value)");
    }

    #[test]
    fn test_bare_field_names_round_trip() {
        for name in [
            "HTTP.Status",
            "@timestamp",
            "_source",
            "field-name",
            "field/path:part",
            "シャボン玉",
            "café",
            "🦀",
            "ADD",
            "IS_NULL",
            "1i32",
            "123",
            "18446744073709551616u64",
        ] {
            let expr = UntypedExpr::variable(name);
            assert_eq!(serialize(&expr), name);
            assert_eq!(deserialize(name).unwrap(), expr);

            let call = UntypedExpr::call(Function::Add, vec![expr]).unwrap();
            let serialized = format!("(ADD {name})");
            assert_eq!(serialize(&call), serialized);
            assert_eq!(deserialize(&serialized).unwrap(), call);
        }
    }

    #[test]
    fn test_literal_names_are_quoted() {
        for name in [
            "none", "true", "false", "1u64", "1i64", "1f64", "+1u64", "1e3f64", "-0f64",
        ] {
            assert!(matches!(
                deserialize(name).unwrap(),
                UntypedExpr::Literal(_)
            ));
            let expr = UntypedExpr::variable(name);
            let serialized = format!("`{name}`");
            assert_eq!(serialize(&expr), serialized);
            assert_eq!(deserialize(&serialized).unwrap(), expr);
        }

        let expr = UntypedExpr::call(
            Function::Add,
            vec![UntypedExpr::variable("1u64"), UntypedExpr::literal(1u64)],
        )
        .unwrap();
        assert_eq!(serialize(&expr), "(ADD `1u64` 1u64)");
        assert_eq!(deserialize("(ADD `1u64` 1u64)").unwrap(), expr);
    }

    #[test]
    fn test_quoted_field_names_round_trip() {
        let cases = [
            ("", "``"),
            ("two words", "`two words`"),
            ("(field)", "`(field)`"),
            ("a\"b", "`a\"b`"),
            ("a`b", "`a\\`b`"),
            ("a\\b", "`a\\\\b`"),
            ("a\n\r\t\0\u{7}", "`a\\n\\r\\t\\0\\u{7}`"),
            ("a\u{2003}b", "`a\u{2003}b`"),
        ];
        for (name, serialized) in cases {
            let expr = UntypedExpr::variable(name);
            assert_eq!(serialize(&expr), serialized);
            assert_eq!(format!("{expr:?}"), serialized);
            assert_eq!(deserialize(serialized).unwrap(), expr);
            let call = UntypedExpr::call(Function::IsNull, vec![expr]).unwrap();
            assert_eq!(deserialize(&serialize(&call)).unwrap(), call);
        }

        // Quoting does not force the canonical serializer to retain quotes.
        assert_eq!(
            serialize(&deserialize("`HTTP.Status`").unwrap()),
            "HTTP.Status"
        );
        assert_eq!(
            deserialize(r"`\u{30b7}\u{30e3}`").unwrap(),
            UntypedExpr::variable("シャ")
        );
        // Backticks inside double quotes still belong to a string literal.
        let string = UntypedExpr::literal("`field`");
        assert_eq!(deserialize(&serialize(&string)).unwrap(), string);
    }

    #[test]
    fn test_field_names_with_every_ascii_character_round_trip() {
        // Tantivy permits every character within a nonempty name that does not
        // start with '-'. Include delimiters, escapes, and control characters.
        for byte in 0u8..=127 {
            let name = format!("field{}tail", char::from(byte));
            let expr = UntypedExpr::variable(&name);
            let serialized = serialize(&expr);
            assert_eq!(deserialize(&serialized).unwrap(), expr, "name: {name:?}");
        }
    }

    #[test]
    fn test_deserialize_errors() {
        let cases = [
            ("", 0, "expected an expression"),
            ("()", 1, "expected a function name"),
            ("(add 1i64)", 1, "must be uppercase"),
            ("(UNKNOWN 1i64)", 1, "unknown function"),
            ("(toto)", 1, "must be uppercase"),
            ("(`ADD` 1u64)", 1, "expected an uppercase function name"),
            ("`unterminated", 0, "unterminated quoted variable"),
            ("`bad\\x`", 4, "unsupported string escape"),
            ("`é\\x`", 3, "unsupported string escape"),
            ("`bad\\", 4, "unterminated string escape"),
            (r"`\u{d800}`", 1, "invalid Unicode scalar value"),
            (r"`\u{}`", 1, "invalid Unicode escape"),
            (
                "`bad\n`",
                4,
                "unescaped control character in quoted variable",
            ),
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
