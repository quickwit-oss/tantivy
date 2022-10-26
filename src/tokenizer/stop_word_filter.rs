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
use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
    words: Arc<FxHashSet<String>>,
}

macro_rules! language {
    ($method:ident, $code:literal) => {
        #[cfg(feature = "stopwords-iso")]
        /// Create a `StopWorldFilter` for the named language
        pub fn $method() -> Self {
            Self::remove(
                include_str!(concat!("stopwords/", $code, ".txt"))
                    .lines()
                    .map(ToOwned::to_owned),
            )
        }
    };
}

impl StopWordFilter {
    /// Creates a `StopWordFilter` given a list of words to remove
    pub fn remove<W: IntoIterator<Item = String>>(words: W) -> StopWordFilter {
        StopWordFilter {
            words: Arc::new(words.into_iter().collect()),
        }
    }

    language!(english, "en");
    language!(french, "fr");
    language!(german, "de");
}

pub struct StopWordFilterStream<'a> {
    words: Arc<FxHashSet<String>>,
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

    #[cfg(feature = "stopwords-iso")]
    #[test]
    fn test_builtin_stop_word_filter() {
        let tokenizer = TextAnalyzer::from(SimpleTokenizer).filter(StopWordFilter::english());

        let mut stream = tokenizer.token_stream("i am a cat. as yet i have no name.");
        assert_eq!(stream.next().unwrap().text, "cat");
        assert_eq!(stream.next(), None);
    }
}
