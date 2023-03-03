/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use tokenizer_api::{BoxTokenStream, BoxableTokenizer, TokenFilter, Tokenizer};

use crate::tokenizer::empty_tokenizer::EmptyTokenizer;

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
pub struct TextAnalyzer {
    tokenizer: Box<dyn BoxableTokenizer>,
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

    /// Creates a token stream for a given `str`.
    pub fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
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
    /// let en_stem = TextAnalyzer::build(SimpleTokenizer)
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
