//! # Example
//! ```
//! extern crate tantivy;
//! use tantivy::tokenizer::*;
//!
//! # fn main() {
//!
//! let tokenizer = SimpleTokenizer
//!   .filter(RemoveLongFilter::limit(5));
//!
//! let mut stream = tokenizer.token_stream("toolong nice");
//! // because `toolong` is more than 5 characters, it is filtered
//! // out of the token stream.
//! assert_eq!(stream.next().unwrap().text, "nice");
//! assert!(stream.next().is_none());
//! # }
//! ```
//!
use super::{Token, TokenFilter, TokenStream};

/// `RemoveLongFilter` removes tokens that are longer
/// than a given number of bytes (in UTF-8 representation).
///
/// It is especially useful when indexing unconstrained content.
/// e.g. Mail containing base-64 encoded pictures etc.
#[derive(Clone)]
pub struct RemoveLongFilter {
    length_limit: usize,
}

impl RemoveLongFilter {
    /// Creates a `RemoveLongFilter` given a limit in bytes of the UTF-8 representation.
    pub fn limit(length_limit: usize) -> RemoveLongFilter {
        RemoveLongFilter { length_limit }
    }
}

impl<TailTokenStream> RemoveLongFilterStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn predicate(&self, token: &Token) -> bool {
        token.text.len() < self.token_length_limit
    }

    fn wrap(
        token_length_limit: usize,
        tail: TailTokenStream,
    ) -> RemoveLongFilterStream<TailTokenStream> {
        RemoveLongFilterStream {
            token_length_limit,
            tail,
        }
    }
}

impl<TailTokenStream> TokenFilter<TailTokenStream> for RemoveLongFilter
where
    TailTokenStream: TokenStream,
{
    type ResultTokenStream = RemoveLongFilterStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        RemoveLongFilterStream::wrap(self.length_limit, token_stream)
    }
}

pub struct RemoveLongFilterStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    token_length_limit: usize,
    tail: TailTokenStream,
}

impl<TailTokenStream> TokenStream for RemoveLongFilterStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }

        false
    }
}
