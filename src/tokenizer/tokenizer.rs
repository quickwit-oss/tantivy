use std::ops::Deref;

/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use tokenizer_api::{BoxTokenStream, TokenFilter, TokenStream, Tokenizer};

use crate::tokenizer::empty_tokenizer::EmptyTokenizer;

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
pub struct TextAnalyzer {
    tokenizer: Box<dyn BoxableTokenizer>,
}

/// A boxable `Tokenizer`, with its `TokenStream` type erased.
pub trait BoxableTokenizer: 'static + Send + Sync {
    /// Creates a boxed token stream for a given `str`.
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;
    /// Clone this tokenizer.
    fn box_clone(&self) -> Box<dyn BoxableTokenizer>;
}

impl<T: Tokenizer> BoxableTokenizer for T {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.token_stream(text).into()
    }
    fn box_clone(&self) -> Box<dyn BoxableTokenizer> {
        Box::new(self.clone())
    }
}

pub struct BoxedTokenizer(Box<dyn BoxableTokenizer>);

impl Clone for BoxedTokenizer {
    fn clone(&self) -> BoxedTokenizer {
        Self(self.0.box_clone())
    }
}

impl Tokenizer for BoxedTokenizer {
    type TokenStream<'a> = Box<dyn TokenStream + 'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.0.box_token_stream(text).into()
    }
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait BoxableTokenFilter: 'static + Send + Sync {
    /// Wraps a Tokenizer and returns a new one.
    fn box_transform(&self, tokenizer: BoxedTokenizer) -> Box<dyn BoxableTokenizer>;
}

impl<T: TokenFilter> BoxableTokenFilter for T {
    fn box_transform(&self, tokenizer: BoxedTokenizer) -> Box<dyn BoxableTokenizer> {
        let tokenizer = self.clone().transform(tokenizer);
        tokenizer.box_clone()
    }
}

pub struct BoxTokenFilter(Box<dyn BoxableTokenFilter>);

impl Deref for BoxTokenFilter {
    type Target = dyn BoxableTokenFilter;

    fn deref(&self) -> &dyn BoxableTokenFilter {
        &*self.0
    }
}

impl<T: TokenFilter> From<T> for BoxTokenFilter {
    fn from(tokenizer: T) -> BoxTokenFilter {
        BoxTokenFilter(Box::new(tokenizer))
    }
}

impl TextAnalyzer {
    /// Builds a new `TextAnalyzer` given a tokenizer and a vector of `BoxTokenFilter`.
    ///
    /// When creating a `TextAnalyzer` from a `Tokenizer` alone, prefer using
    /// `TextAnalyzer::from(tokenizer)`.
    /// When creating a `TextAnalyzer` from a `Tokenizer` and a static set of `TokenFilter`,
    /// prefer using `TextAnalyzer::builder(tokenizer).filter(token_filter).build()`.
    pub fn build<T: Tokenizer>(
        tokenizer: T,
        boxed_token_filters: Vec<BoxTokenFilter>,
    ) -> TextAnalyzer {
        let mut boxed_tokenizer = BoxedTokenizer(Box::new(tokenizer));
        for filter in boxed_token_filters.into_iter() {
            let filtered_boxed_tokenizer = filter.box_transform(boxed_tokenizer);
            boxed_tokenizer = BoxedTokenizer(filtered_boxed_tokenizer);
        }
        TextAnalyzer {
            tokenizer: boxed_tokenizer.0,
        }
    }

    /// Create a new TextAnalyzerBuilder
    pub fn builder<T: Tokenizer>(tokenizer: T) -> TextAnalyzerBuilder<T> {
        TextAnalyzerBuilder { tokenizer }
    }

    /// Creates a token stream for a given `str`.
    pub fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.tokenizer.box_token_stream(text)
    }
}

impl Clone for TextAnalyzer {
    fn clone(&self) -> Self {
        TextAnalyzer {
            tokenizer: self.tokenizer.box_clone(),
        }
    }
}

impl Default for TextAnalyzer {
    fn default() -> TextAnalyzer {
        TextAnalyzer::from(EmptyTokenizer)
    }
}

impl<T: Tokenizer + Clone> From<T> for TextAnalyzer {
    fn from(tokenizer: T) -> Self {
        TextAnalyzer::builder(tokenizer).build()
    }
}

/// Builder helper for [`TextAnalyzer`]
pub struct TextAnalyzerBuilder<T: Tokenizer> {
    tokenizer: T,
}

impl<T: Tokenizer> TextAnalyzerBuilder<T> {
    /// Appends a token filter to the current builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tantivy::tokenizer::*;
    ///
    /// let en_stem = TextAnalyzer::builder(SimpleTokenizer::default())
    ///     .filter(RemoveLongFilter::limit(40))
    ///     .filter(LowerCaser)
    ///     .filter(Stemmer::default())
    ///     .build();
    /// ```
    pub fn filter<F: TokenFilter>(self, token_filter: F) -> TextAnalyzerBuilder<F::Tokenizer<T>> {
        TextAnalyzerBuilder {
            tokenizer: token_filter.transform(self.tokenizer),
        }
    }

    /// Finalize building the TextAnalyzer
    pub fn build(self) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: Box::new(self.tokenizer),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::tokenizer::{AlphaNumOnlyFilter, LowerCaser, RemoveLongFilter, WhitespaceTokenizer};

    #[test]
    fn test_text_analyzer_builder() {
        let mut analyzer = TextAnalyzer::builder(WhitespaceTokenizer::default())
            .filter(AlphaNumOnlyFilter)
            .filter(RemoveLongFilter::limit(6))
            .filter(LowerCaser)
            .build();
        let mut stream = analyzer.token_stream("- first bullet point");
        assert_eq!(stream.next().unwrap().text, "first");
        assert_eq!(stream.next().unwrap().text, "point");
    }

    #[test]
    fn test_text_analyzer_with_filters_boxed() {
        let mut analyzer = TextAnalyzer::build(
            WhitespaceTokenizer::default(),
            vec![
                BoxTokenFilter::from(AlphaNumOnlyFilter),
                BoxTokenFilter::from(LowerCaser),
                BoxTokenFilter::from(RemoveLongFilter::limit(6)),
            ],
        );
        let mut stream = analyzer.token_stream("- first bullet point");
        assert_eq!(stream.next().unwrap().text, "first");
        assert_eq!(stream.next().unwrap().text, "point");
    }
}
