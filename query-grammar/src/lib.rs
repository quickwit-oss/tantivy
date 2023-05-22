#![allow(clippy::derive_partial_eq_without_eq)]

mod occur;
mod query_grammar;
mod user_input_ast;
use combine::parser::Parser;

pub use crate::occur::Occur;
use crate::query_grammar::parse_to_ast;
pub use crate::user_input_ast::{
    Delimiter, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};

pub struct Error;

pub fn parse_query(query: &str) -> Result<UserInputAst, Error> {
    let (user_input_ast, _remaining) = parse_to_ast().parse(query).map_err(|_| Error)?;
    Ok(user_input_ast)
}
