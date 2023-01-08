/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use tokenizer_api::{BoxTokenFilter, BoxTokenStream, Tokenizer};

use crate::tokenizer::empty_tokenizer::EmptyTokenizer;

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
///
/// It simply wraps a `Tokenizer` and a list of `TokenFilter` that are applied sequentially.
pub struct TextAnalyzer {
    tokenizer: Box<dyn Tokenizer>,
    token_filters: Vec<BoxTokenFilter>,
}

impl Default for TextAnalyzer {
    fn default() -> TextAnalyzer {
        TextAnalyzer::from(EmptyTokenizer)
    }
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
    #[must_use]
    pub fn filter<F: Into<BoxTokenFilter>>(mut self, token_filter: F) -> Self {
        self.token_filters.push(token_filter.into());
        self
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
