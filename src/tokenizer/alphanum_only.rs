//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let tokenizer = TextAnalyzer::from(RawTokenizer)
//!   .filter(AlphaNumOnlyFilter);
//!
//! let mut stream = tokenizer.token_stream("hello there");
//! // is none because the raw filter emits one token that
//! // contains a space
//! assert!(stream.next().is_none());
//!
//! let tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!   .filter(AlphaNumOnlyFilter);
//!
//! let mut stream = tokenizer.token_stream("hello there 💣");
//! assert!(stream.next().is_some());
//! assert!(stream.next().is_some());
//! // the "emoji" is dropped because its not an alphanum
//! assert!(stream.next().is_none());
//! ```
use super::{Token, TokenFilter, TokenStream};

/// `TokenFilter` that removes all tokens that contain non
/// ascii alphanumeric characters.
#[derive(Clone)]
pub struct AlphaNumOnlyFilter;

impl TokenFilter for AlphaNumOnlyFilter {
    fn transform(&mut self, token: Token) -> Option<Token> {
        if token.text.chars().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        Some(token)
    }
}
