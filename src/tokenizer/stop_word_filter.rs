//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!   .filter(StopWordFilter::remove(vec!["the".to_string(), "is".to_string()]));
//!
//! let mut stream = tokenizer.token_stream("the fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! ```
use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;
use fnv::FnvHasher;
use std::collections::HashSet;
use std::hash::BuildHasherDefault;

// configure our hashers for SPEED
type StopWordHasher = BuildHasherDefault<FnvHasher>;
type StopWordHashSet = HashSet<String, StopWordHasher>;

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
    words: StopWordHashSet,
}

impl StopWordFilter {
    /// Creates a `StopWordFilter` given a list of words to remove
    pub fn remove(words: Vec<String>) -> StopWordFilter {
        let mut set = StopWordHashSet::default();

        for word in words {
            set.insert(word);
        }

        StopWordFilter { words: set }
    }

    fn english() -> StopWordFilter {
        let words: [&'static str; 33] = [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
            "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with",
        ];

        StopWordFilter::remove(words.iter().map(|&s| s.to_string()).collect())
    }
}

pub struct StopWordFilterStream<'a> {
    words: StopWordHashSet,
    tail: BoxTokenStream<'a>,
}

impl TokenFilter for StopWordFilter {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        BoxTokenStream::from(StopWordFilterStream {
            words: self.words.clone(),
            tail: token_stream,
        })
    }
}

impl<'a> StopWordFilterStream<'a> {
    fn predicate(&self, token: &Token) -> bool {
        !self.words.contains(&token.text)
    }
}

impl<'a> TokenStream for StopWordFilterStream<'a> {
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

impl Default for StopWordFilter {
    fn default() -> StopWordFilter {
        StopWordFilter::english()
    }
}
