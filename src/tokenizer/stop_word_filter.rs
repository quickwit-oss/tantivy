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
use std::collections::HashSet;
use std::hash::BuildHasherDefault;

use fnv::FnvHasher;

use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;

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

#[cfg(test)]
mod tests {
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{SimpleTokenizer, StopWordFilter, TextAnalyzer, Token};

    #[test]
    fn test_stop_word() {
        let tokens = token_stream_helper("i am a cat. as yet i have no name.");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 3, "cat", 7, 10);
        assert_token(&tokens[1], 5, "yet", 15, 18);
        assert_token(&tokens[2], 7, "have", 21, 25);
        assert_token(&tokens[3], 8, "no", 26, 28);
        assert_token(&tokens[4], 9, "name", 29, 33);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let stops = vec![
            "a".to_string(),
            "as".to_string(),
            "am".to_string(),
            "i".to_string(),
        ];
        let a = TextAnalyzer::from(SimpleTokenizer).filter(StopWordFilter::remove(stops));
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
