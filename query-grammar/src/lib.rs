#![allow(clippy::derive_partial_eq_without_eq)]

mod infallible;
mod occur;
mod query_grammar;
mod user_input_ast;

pub use crate::infallible::LenientError;
pub use crate::occur::Occur;
use crate::query_grammar::{parse_to_ast, parse_to_ast_lenient};
pub use crate::user_input_ast::{
    Delimiter, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

#[derive(Debug)]
pub struct Error;

/// Parse a query
pub fn parse_query(query: &str) -> Result<UserInputAst, Error> {
    let (_remaining, user_input_ast) = parse_to_ast(query).map_err(|_| Error)?;
    Ok(user_input_ast)
}

/// Parse a query, trying to recover from syntax errors, and giving hints toward fixing errors.
pub fn parse_query_lenient(query: &str) -> (UserInputAst, Vec<LenientError>) {
    parse_to_ast_lenient(query)
}

#[cfg(test)]
mod tests {
    use crate::{parse_query, parse_query_lenient};

    #[test]
    fn test_parse_query_serialization() {
        let ast = parse_query("title:hello").unwrap();
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"Leaf":{"Literal":{"field_name":"title","phrase":"hello","delimiter":"None","slop":0,"prefix":false}}}"#
        );
    }

    #[test]
    fn test_parse_query_wrong_query() {
        assert!(parse_query("title:").is_err());
    }

    #[test]
    fn test_parse_query_lenient_wrong_query() {
        let (_, errors) = parse_query_lenient("title:");
        assert!(errors.len() == 1);
        let json = serde_json::to_string(&errors).unwrap();
        assert_eq!(json, r#"[{"pos":6,"message":"expected word"}]"#);
    }
}
