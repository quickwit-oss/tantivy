use crate::tokenizer::TokenStreamChain;
use serde::{Deserialize, Serialize};
/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};

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
pub struct TextAnalyzer {
    tokenizer: Box<dyn Tokenizer>,
    token_filters: Vec<BoxTokenFilter>,
}

impl<T: Tokenizer> From<T> for TextAnalyzer {
    fn from(tokenizer: T) -> Self {
        TextAnalyzer::new(tokenizer, Vec::new())
    }
}

impl TextAnalyzer {
    /// Creates a new `TextAnalyzer` given a tokenizer and a vector of `BoxTokenFilter`.
    ///
    /// When creating a `TextAnalyzer` from a `Tokenizer` alone, prefer using
    /// `TextAnalyzer::from(tokenizer)`.
    pub fn new<T: Tokenizer>(tokenizer: T, token_filters: Vec<BoxTokenFilter>) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: Box::new(tokenizer),
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
    pub fn filter<F: Into<BoxTokenFilter>>(mut self, token_filter: F) -> Self {
        self.token_filters.push(token_filter.into());
        self
    }

    /// Tokenize an array`&str`
    ///
    /// The resulting `BoxTokenStream` is equivalent to what would be obtained if the &str were
    /// one concatenated `&str`, with an artificial position gap of `2` between the different fields
    /// to prevent accidental `PhraseQuery` to match accross two terms.
    pub fn token_stream_texts<'a>(&self, texts: &'a [&'a str]) -> BoxTokenStream<'a> {
        assert!(!texts.is_empty());
        if texts.len() == 1 {
            self.token_stream(texts[0])
        } else {
            let mut offsets = vec![];
            let mut total_offset = 0;
            for &text in texts {
                offsets.push(total_offset);
                total_offset += text.len();
            }
            let token_streams: Vec<BoxTokenStream<'a>> = texts
                .iter()
                .cloned()
                .map(|text| self.token_stream(text))
                .collect();
            From::from(TokenStreamChain::new(offsets, token_streams))
        }
    }

    /// Creates a token stream for a given `str`.
    pub fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        let mut token_stream = self.tokenizer.token_stream(text);
        for token_filter in &self.token_filters {
            token_stream = token_filter.transform(token_stream);
        }
        token_stream
    }
}

impl Clone for TextAnalyzer {
    fn clone(&self) -> Self {
        TextAnalyzer {
            tokenizer: self.tokenizer.box_clone(),
            token_filters: self
                .token_filters
                .iter()
                .map(|token_filter| token_filter.box_clone())
                .collect(),
        }
    }
}

/// `Tokenizer` are in charge of splitting text into a stream of token
/// before indexing.
///
/// See the [module documentation](./index.html) for more detail.
///
/// # Warning
///
/// This API may change to use associated types.
pub trait Tokenizer: 'static + Send + Sync + TokenizerClone {
    /// Creates a token stream for a given `str`.
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a>;
}

pub trait TokenizerClone {
    fn box_clone(&self) -> Box<dyn Tokenizer>;
}

impl<T: Tokenizer + Clone> TokenizerClone for T {
    fn box_clone(&self) -> Box<dyn Tokenizer> {
        Box::new(self.clone())
    }
}

impl<'a> TokenStream for Box<dyn TokenStream + 'a> {
    fn advance(&mut self) -> bool {
        let token_stream: &mut dyn TokenStream = self.borrow_mut();
        token_stream.advance()
    }

    fn token<'b>(&'b self) -> &'b Token {
        let token_stream: &'b (dyn TokenStream + 'a) = self.borrow();
        token_stream.token()
    }

    fn token_mut<'b>(&'b mut self) -> &'b mut Token {
        let token_stream: &'b mut (dyn TokenStream + 'a) = self.borrow_mut();
        token_stream.token_mut()
    }
}

/// Simple wrapper of `Box<dyn TokenStream + 'a>`.
///
/// See `TokenStream` for more information.
pub struct BoxTokenStream<'a>(Box<dyn TokenStream + 'a>);

impl<'a, T> From<T> for BoxTokenStream<'a>
where
    T: TokenStream + 'a,
{
    fn from(token_stream: T) -> BoxTokenStream<'a> {
        BoxTokenStream(Box::new(token_stream))
    }
}

impl<'a> Deref for BoxTokenStream<'a> {
    type Target = dyn TokenStream + 'a;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
impl<'a> DerefMut for BoxTokenStream<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

/// Simple wrapper of `Box<dyn TokenFilter + 'a>`.
///
/// See `TokenStream` for more information.
pub struct BoxTokenFilter(Box<dyn TokenFilter>);

impl Deref for BoxTokenFilter {
    type Target = dyn TokenFilter;

    fn deref(&self) -> &dyn TokenFilter {
        &*self.0
    }
}

impl<T: TokenFilter> From<T> for BoxTokenFilter {
    fn from(tokenizer: T) -> BoxTokenFilter {
        BoxTokenFilter(Box::new(tokenizer))
    }
}

/// `TokenStream` is the result of the tokenization.
///
/// It consists consumable stream of `Token`s.
///
/// # Example
///
/// ```
/// use tantivy::tokenizer::*;
///
/// let tokenizer = TextAnalyzer::from(SimpleTokenizer)
///        .filter(RemoveLongFilter::limit(40))
///        .filter(LowerCaser);
/// let mut token_stream = tokenizer.token_stream("Hello, happy tax payer");
/// {
///     let token = token_stream.next().unwrap();
///     assert_eq!(&token.text, "hello");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 5);
///     assert_eq!(token.position, 0);
/// }
/// {
///     let token = token_stream.next().unwrap();
///     assert_eq!(&token.text, "happy");
///     assert_eq!(token.offset_from, 7);
///     assert_eq!(token.offset_to, 12);
///     assert_eq!(token.position, 1);
/// }
/// ```
///
pub trait TokenStream {
    /// Advance to the next token
    ///
    /// Returns false if there are no other tokens.
    fn advance(&mut self) -> bool;

    /// Returns a reference to the current token.
    fn token(&self) -> &Token;

    /// Returns a mutable reference to the current token.
    fn token_mut(&mut self) -> &mut Token;

    /// Helper to iterate over tokens. It
    /// simply combines a call to `.advance()`
    /// and `.token()`.
    ///
    /// ```
    /// use tantivy::tokenizer::*;
    ///
    /// let tokenizer = TextAnalyzer::from(SimpleTokenizer)
    ///       .filter(RemoveLongFilter::limit(40))
    ///       .filter(LowerCaser);
    /// let mut token_stream = tokenizer.token_stream("Hello, happy tax payer");
    /// while let Some(token) = token_stream.next() {
    ///     println!("Token {:?}", token.text);
    /// }
    /// ```
    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        } else {
            None
        }
    }

    /// Helper function to consume the entire `TokenStream`
    /// and push the tokens to a sink function.
    ///
    /// Remove this.
    fn process(&mut self, sink: &mut dyn FnMut(&Token)) -> u32 {
        let mut num_tokens_pushed = 0u32;
        while self.advance() {
            sink(self.token());
            num_tokens_pushed += 1u32;
        }
        num_tokens_pushed
    }
}

pub trait TokenFilterClone {
    fn box_clone(&self) -> BoxTokenFilter;
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: 'static + Send + Sync + TokenFilterClone {
    /// Wraps a token stream and returns the modified one.
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a>;
}

impl<T: TokenFilter + Clone> TokenFilterClone for T {
    fn box_clone(&self) -> BoxTokenFilter {
        BoxTokenFilter::from(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::Token;

    #[test]
    fn clone() {
        let t1 = Token {
            position: 1,
            offset_from: 2,
            offset_to: 3,
            text: "abc".to_string(),
            position_length: 1,
        };
        let t2 = t1.clone();

        assert_eq!(t1.position, t2.position);
        assert_eq!(t1.offset_from, t2.offset_from);
        assert_eq!(t1.offset_to, t2.offset_to);
        assert_eq!(t1.text, t2.text);
    }
}
