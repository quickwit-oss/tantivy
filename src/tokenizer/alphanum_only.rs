//! # Example
//! ```
//! extern crate tantivy;
//! use tantivy::tokenizer::*;
//!
//! # fn main() {
//!
//! let tokenizer = RawTokenizer
//!   .filter(AlphaNumOnlyFilter);
//!
//! let mut stream = tokenizer.token_stream("hello there");
//! // is none because the raw filter emits one token that
//! // contains a space
//! assert!(stream.next().is_none());
//!
//! let tokenizer = SimpleTokenizer
//!   .filter(AlphaNumOnlyFilter);
//!
//! let mut stream = tokenizer.token_stream("hello there 💣");
//! assert!(stream.next().is_some());
//! assert!(stream.next().is_some());
//! // the "emoji" is dropped because its not an alphanum
//! assert!(stream.next().is_none());
//! # }
//! ```
use super::{Token, TokenFilter, TokenStream};

/// `TokenFilter` that removes all tokens that contain non
/// ascii alphanumeric characters.
#[derive(Clone)]
pub struct AlphaNumOnlyFilter;

pub struct AlphaNumOnlyFilterStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    tail: TailTokenStream,
}

impl<TailTokenStream> AlphaNumOnlyFilterStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn predicate(&self, token: &Token) -> bool {
        token.text.chars().all(|c| c.is_ascii_alphanumeric())
    }

    fn wrap(tail: TailTokenStream) -> AlphaNumOnlyFilterStream<TailTokenStream> {
        AlphaNumOnlyFilterStream { tail }
    }
}

impl<TailTokenStream> TokenFilter<TailTokenStream> for AlphaNumOnlyFilter
where
    TailTokenStream: TokenStream,
{
    type ResultTokenStream = AlphaNumOnlyFilterStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        AlphaNumOnlyFilterStream::wrap(token_stream)
    }
}

impl<TailTokenStream> TokenStream for AlphaNumOnlyFilterStream<TailTokenStream>
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
