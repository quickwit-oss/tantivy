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
#[cfg(feature = "stopwords")]
#[rustfmt::skip]
mod stopwords;

use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
    words: Arc<FxHashSet<String>>,
}

impl StopWordFilter {
    /// Creates a `StopWordFilter` given a list of words to remove
    pub fn remove<W: IntoIterator<Item = String>>(words: W) -> StopWordFilter {
        StopWordFilter {
            words: Arc::new(words.into_iter().collect()),
        }
    }

    fn from_word_list(words: &[&str]) -> Self {
        Self::remove(words.iter().map(|&word| word.to_owned()))
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Danish language
    pub fn danish() -> Self {
        Self::from_word_list(stopwords::DANISH)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Dutch language
    pub fn dutch() -> Self {
        Self::from_word_list(stopwords::DUTCH)
    }

    /// Create a `StopWorldFilter` for the English language
    pub fn english() -> Self {
        // This is the same list of words used by the Apache-licensed Lucene project,
        // c.f. https://github.com/apache/lucene/blob/d5d6dc079395c47cd6d12dcce3bcfdd2c7d9dc63/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java#L46
        const WORDS: &[&str] = &[
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
            "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with",
        ];

        Self::from_word_list(WORDS)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Finnish language
    pub fn finnish() -> Self {
        Self::from_word_list(stopwords::FINNISH)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the French language
    pub fn french() -> Self {
        Self::from_word_list(stopwords::FRENCH)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the German language
    pub fn german() -> Self {
        Self::from_word_list(stopwords::GERMAN)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Italian language
    pub fn italian() -> Self {
        Self::from_word_list(stopwords::ITALIAN)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Norwegian language
    pub fn norwegian() -> Self {
        Self::from_word_list(stopwords::NORWEGIAN)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Portuguese language
    pub fn portuguese() -> Self {
        Self::from_word_list(stopwords::PORTUGUESE)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Russian language
    pub fn russian() -> Self {
        Self::from_word_list(stopwords::RUSSIAN)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Spanish language
    pub fn spanish() -> Self {
        Self::from_word_list(stopwords::SPANISH)
    }

    #[cfg(feature = "stopwords")]
    /// Create a `StopWorldFilter` for the Swedish language
    pub fn swedish() -> Self {
        Self::from_word_list(stopwords::SWEDISH)
    }
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
