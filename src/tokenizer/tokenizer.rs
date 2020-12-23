use crate::tokenizer::TokenStreamChain;
use serde::{Deserialize, Serialize};
/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.

/// Token
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Token {
    /// Offset (byte index) of the first character of the token.
    /// Offsets shall not be modified by token filters.
    pub offset_from: usize,
    /// Offset (byte index) of the last character of the token + 1.
    /// The text that generated the token should be obtained by
    /// &text[token.offset_from..token.offset_to]
    pub offset_to: usize,
    /// Position, expressed in number of tokens.
    pub position: usize,
    /// Actual text content of the token.
    pub text: String,
    /// Is the length expressed in term of number of original tokens.
    pub position_length: usize,
}

impl Default for Token {
    fn default() -> Token {
        Token {
            offset_from: 0,
            offset_to: 0,
            position: usize::max_value(),
            text: String::with_capacity(200),
            position_length: 1,
        }
    }
}

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
///
/// It simply wraps a `Tokenizer` and a list of `TokenFilter` that are applied sequentially.
#[derive(Clone)]
pub struct TokenStream<'a, I> {
    tokens: I,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl<'a, I> Iterator for TokenStream<'a, I>
where
    I: Iterator<Item = Token>,
{
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(token) = self.tokens.next() {
            if self.filters.all(|filter| filter(&token)) {
                return Some(token);
            }
        }
        None
    }
}

impl<'a, I> TokenStream<'a, I>
where
    I: Iterator<Item = Token>,
{
    /// Creates a new `TextAnalyzer` given a tokenizer and a vector of `Box<dyn TokenFilter>`.
    ///
    /// When creating a `TextAnalyzer` from a `Tokenizer` alone, prefer using
    /// `TextAnalyzer::from(tokenizer)`.
    pub fn new<T: Tokenizer<'a, Iter = I>>(
        tokenizer: T,
        text: &str,
        token_filters: Vec<Box<dyn TokenFilter>>,
    ) -> TokenStream<'a, I> {
        TokenStream {
            tokens: tokenizer.token_stream(text),
            token_filters,
        }
    }

    /// Appends a token filter to the current tokenizer.
    ///
    /// The method consumes the current `TokenStream` and returns a
    /// new one.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tantivy::tokenizer::*;
    ///
    /// let en_stem = TextAnalyzer::from(SimpleTokenizer)
    ///     .filter(RemoveLongFilter::limit(40))
    ///     .filter(LowerCaser)
    ///     .filter(Stemmer::default());
    /// ```
    ///
    pub fn filter<F: TokenFilter>(mut self, token_filter: F) -> Self {
        self.token_filters.push(Box::new(token_filter));
        self
    }

    // /// Tokenize an array`&str`
    // ///
    // /// The resulting `BoxTokenStream` is equivalent to what would be obtained if the &str were
    // /// one concatenated `&str`, with an artificial position gap of `2` between the different fields
    // /// to prevent accidental `PhraseQuery` to match accross two terms.

    // /// Creates a token stream for a given `str`.
    // pub fn token_stream<'a>(&self, text: &'a str) -> Box<dyn TokenStream + 'a> {
    //     let mut token_stream = self.tokenizer.token_stream(text);
    //     for token_filter in &self.token_filters {
    //         token_stream = token_filter.transform(token_stream);
    //     }
    //     token_stream
    // }
}

// impl<'a,I: Clone> Clone for Tokens<'a,I> {
//     fn clone(&self) -> Self {
//         Tokens {
//             tokenizer: self.tokenizer.box_clone(),
//             token_filters: self
//                 .token_filters
//                 .iter()
//                 .map(|token_filter| token_filter.box_clone())
//                 .collect(),
//         }
//     }
// }

/// `Tokenizer` are in charge of splitting text into a stream of token
/// before indexing.
///
/// See the [module documentation](./index.html) for more detail.
///
/// # Warning
///
/// This API may change to use associated types.
pub trait Tokenizer<'a>: 'static + Send + Sync + Clone {
    type Iter: Iterator<Item = Token> + 'a;
    /// Creates a token stream for a given `str`.
    fn token_stream(&self, text: &'a str) -> Self::Iter;
    fn token_stream_texts(&self, texts: &'a [&str]) -> Self::Iter {
        debug_assert!(!texts.is_empty());
        let mut streams_with_offsets = vec![];
        let mut total_offset = 0;
        for &text in texts {
            streams_with_offsets.push((self.token_stream(text), total_offset));
            total_offset += text.len();
        }
        TokenStreamChain::new(streams_with_offsets)
    }
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: Fn(&Token) -> bool + 'static + Send + Sync + TokenFilterClone {}

pub trait TokenFilterClone {
    fn box_clone(&self) -> Box<dyn TokenFilter>;
}

impl<T: TokenFilter + Clone> TokenFilterClone for T {
    fn box_clone(&self) -> Box<dyn TokenFilter> {
        Box::new(self.clone())
    }
}

// #[cfg(test)]
// mod test {
//     use super::Token;

//     #[test]
//     fn clone() {
//         let t1 = Token {
//             position: 1,
//             offset_from: 2,
//             offset_to: 3,
//             text: "abc".to_string(),
//             position_length: 1,
//         };
//         let t2 = t1.clone();

//         assert_eq!(t1.position, t2.position);
//         assert_eq!(t1.offset_from, t2.offset_from);
//         assert_eq!(t1.offset_to, t2.offset_to);
//         assert_eq!(t1.text, t2.text);
//     }
// }
