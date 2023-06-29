/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use tokenizer_api::{BoxTokenStream, TokenFilter, Tokenizer};

use crate::tokenizer::empty_tokenizer::EmptyTokenizer;

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
pub struct TextAnalyzer {
    tokenizer: Box<dyn BoxableTokenizer>,
}

/// Wrapper to avoid recursive acalls of `box_token_stream`.
#[derive(Clone)]
struct BoxedTokenizer(Box<dyn BoxableTokenizer>);

impl Tokenizer for BoxedTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.0.box_token_stream(text)
    }
}

impl Clone for Box<dyn BoxableTokenizer> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

fn add_filter<F: TokenFilter>(tokenizer: BoxedTokenizer, filter: F) -> BoxedTokenizer {
    let filtered_tokenizer = filter.transform(tokenizer);
    BoxedTokenizer(Box::new(filtered_tokenizer))
}


/// A boxable `Tokenizer`, with its `TokenStream` type erased.
trait BoxableTokenizer: 'static + Send + Sync {
    /// Creates a boxed token stream for a given `str`.
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;
    /// Clone this tokenizer.
    fn box_clone(&self) -> Box<dyn BoxableTokenizer>;
}

impl<T: Tokenizer> BoxableTokenizer for T {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self.token_stream(text))
    }
    fn box_clone(&self) -> Box<dyn BoxableTokenizer> {
        Box::new(self.clone())
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

impl TextAnalyzer {
    /// Create a new TextAnalyzerBuilder
    pub fn builder<T: Tokenizer>(tokenizer: T) -> TextAnalyzerBuilder<T> {
        TextAnalyzerBuilder { tokenizer }
    }

    /// TODO
    pub fn dynamic_filter_builder<T: Tokenizer>(tokenizer: T) -> DynamicTextAnalyzerBuilder {
        DynamicTextAnalyzerBuilder::new(tokenizer)
    }

    /// Creates a token stream for a given `str`.
    pub fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        self.tokenizer.box_token_stream(text)
    }
}

/// Builder helper for [`TextAnalyzer`]
pub struct TextAnalyzerBuilder<T> {
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

/// Builder helper for [`TextAnalyzer`] with dynamic filters.
pub struct DynamicTextAnalyzerBuilder {
    tokenizer: BoxedTokenizer,
}

impl DynamicTextAnalyzerBuilder {
    pub fn new<T: Tokenizer>(tokenizer: T) -> DynamicTextAnalyzerBuilder {
        DynamicTextAnalyzerBuilder { tokenizer: BoxedTokenizer(Box::new(tokenizer)) }
    }

    pub fn filter<F: TokenFilter>(self, filter: F) -> DynamicTextAnalyzerBuilder {
        DynamicTextAnalyzerBuilder {
            tokenizer: add_filter(self.tokenizer, filter),
        }
    }

    pub fn build(self) -> TextAnalyzer {
        TextAnalyzer {
            tokenizer: self.tokenizer.0,
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
        let mut analyzer = TextAnalyzer::dynamic_filter_builder(WhitespaceTokenizer::default())
            .filter(AlphaNumOnlyFilter)
            .filter(RemoveLongFilter::limit(6))
            .filter(LowerCaser)
            .build();
        let mut stream = analyzer.token_stream("- first bullet point");
        assert_eq!(stream.next().unwrap().text, "first");
        assert_eq!(stream.next().unwrap().text, "point");
    }
}
