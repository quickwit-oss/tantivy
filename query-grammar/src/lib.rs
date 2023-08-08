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
