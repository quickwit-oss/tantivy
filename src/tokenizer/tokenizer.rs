use dyn_clone::DynClone;
/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use tokenizer_api::{FilteredTokenizer, TokenFilter, TokenStream, Tokenizer};

use crate::tokenizer::empty_tokenizer::EmptyTokenizer;

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
#[derive(Clone)]
pub struct TextAnalyzer {
    tokenizer: Box<dyn BoxableTokenizer>,
    token_filters: Vec<BoxTokenFilter>,
}

/// A boxable `Tokenizer`, with its `TokenStream` type erased.
trait BoxableTokenizer: 'static + Send + Sync + DynClone {
    /// Creates a boxed token stream for a given `str`.
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> Box<dyn TokenStream + 'a>;
}

impl<T: Tokenizer> BoxableTokenizer for T {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> Box<dyn TokenStream + 'a> {
        Box::new(self.token_stream(text))
    }
}

dyn_clone::clone_trait_object!(BoxableTokenizer);

/// A boxable `TokenFilter`, with its `Tokenizer` type erased.
trait BoxableTokenFilter: 'static + Send + Sync + DynClone {
    /// Transforms a boxed token stream into a new one.
    fn box_filter<'a>(
        &'a mut self,
        token_stream: Box<dyn TokenStream + 'a>,
    ) -> Box<dyn TokenStream + 'a>;
}

impl<T: TokenFilter> BoxableTokenFilter for T {
    fn box_filter<'a>(
        &'a mut self,
        token_stream: Box<dyn TokenStream + 'a>,
    ) -> Box<dyn TokenStream + 'a> {
        Box::new(self.filter(token_stream))
    }
}

dyn_clone::clone_trait_object!(BoxableTokenFilter);

/// Simple wrapper of `Box<dyn TokenFilter + 'a>`.
///
/// See [`TokenFilter`] for more information.
#[derive(Clone)]
pub struct BoxTokenFilter(Box<dyn BoxableTokenFilter>);

impl<T: TokenFilter> From<T> for BoxTokenFilter {
    fn from(tokenizer: T) -> BoxTokenFilter {
        BoxTokenFilter(Box::new(tokenizer))
    }
}

impl TextAnalyzer {
    /// Builds a new `TextAnalyzer` given a tokenizer and a vector of `BoxTokenFilter`.
    ///
    /// When creating a `TextAnalyzer` from a `Tokenizer` and a static set of `TokenFilter`,
    /// prefer using `TextAnalyzer::builder(tokenizer).filter(token_filter).build()` as it
    /// will be more performant and create less boxes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tantivy::tokenizer::*;
    ///
    /// let en_stem = TextAnalyzer::new(
    ///     SimpleTokenizer::default(),
    ///     vec![
    ///        BoxTokenFilter::from(RemoveLongFilter::limit(40)),
    ///        BoxTokenFilter::from(LowerCaser),
    ///        BoxTokenFilter::from(Stemmer::default()),
    ///     ]);
    /// ```
    pub fn new<T: Tokenizer>(tokenizer: T, token_filters: Vec<BoxTokenFilter>) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: Box::new(tokenizer),
            token_filters,
        }
    }

    /// Create a new TextAnalyzerBuilder.
    pub fn builder<T: Tokenizer>(tokenizer: T) -> TextAnalyzerBuilder<T> {
        TextAnalyzerBuilder { tokenizer }
    }

    /// Creates a token stream for a given `str`.
    pub fn token_stream<'a>(&'a mut self, text: &'a str) -> Box<dyn TokenStream + 'a> {
        let mut token_stream = self.tokenizer.box_token_stream(text);
        for token_filter in self.token_filters.iter_mut() {
            token_stream = token_filter.0.box_filter(token_stream);
        }
        token_stream
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
    pub fn filter<F: TokenFilter>(
        self,
        token_filter: F,
    ) -> TextAnalyzerBuilder<FilteredTokenizer<T, F>> {
        TextAnalyzerBuilder {
            tokenizer: token_filter.transform(self.tokenizer),
        }
    }

    /// Finalize building the TextAnalyzer
    pub fn build(self) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: Box::new(self.tokenizer),
            token_filters: Vec::new(),
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
            .filter(LowerCaser::default())
            .build();
        let mut stream = analyzer.token_stream("- first bullet point");
        assert_eq!(stream.next().unwrap().text, "first");
        assert_eq!(stream.next().unwrap().text, "point");
    }

    #[test]
    fn test_text_analyzer_with_filters_boxed() {
        let mut analyzer = TextAnalyzer::new(
            WhitespaceTokenizer::default(),
            vec![
                BoxTokenFilter::from(AlphaNumOnlyFilter),
                BoxTokenFilter::from(LowerCaser::default()),
                BoxTokenFilter::from(RemoveLongFilter::limit(6)),
            ],
        );
        let mut stream = analyzer.token_stream("- first bullet point");
        assert_eq!(stream.next().unwrap().text, "first");
        assert_eq!(stream.next().unwrap().text, "point");
    }
}
