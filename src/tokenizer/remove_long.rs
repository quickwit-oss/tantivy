//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!   .filter(RemoveLongFilter::limit(5));
//!
//! let mut stream = tokenizer.token_stream("toolong nice");
//! // because `toolong` is more than 5 characters, it is filtered
//! // out of the token stream.
//! assert_eq!(stream.next().unwrap().text, "nice");
//! assert!(stream.next().is_none());
//! ```
use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;

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

impl<'a> RemoveLongFilterStream<'a> {
    fn predicate(&self, token: &Token) -> bool {
        token.text.len() < self.token_length_limit
    }
}

impl TokenFilter for RemoveLongFilter {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        BoxTokenStream::from(RemoveLongFilterStream {
            token_length_limit: self.length_limit,
            tail: token_stream,
        })
    }
}

pub struct RemoveLongFilterStream<'a> {
    token_length_limit: usize,
    tail: BoxTokenStream<'a>,
}

impl<'a> TokenStream for RemoveLongFilterStream<'a> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_remove_long() {
        let tokens = token_stream_helper("hello tantivy, happy searching!");
        assert_eq!(tokens.len(), 2);
        assert_token(&tokens[0], 0, "hello", 0, 5);
        assert_token(&tokens[1], 2, "happy", 15, 20);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let a = TextAnalyzer::from(SimpleTokenizer).filter(RemoveLongFilter::limit(6));
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
