use crate::tokenizer::{DynTokenStreamChain, TokenStreamChain};
use serde::{Deserialize, Serialize};
/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.

pub trait TextAnalyzerClone {
    fn box_clone(&self) -> Box<dyn TextAnalyzerT>;
}

/// 'Top-level' trait hiding concrete types, below which static dispatch occurs.
pub trait TextAnalyzerT: 'static + Send + Sync + TextAnalyzerClone {
    /// 'Top-level' dynamic dispatch function hiding concrete types of the staticly
    /// dispatched `token_stream` from the `Tokenizer` trait.
    fn token_stream(&self, text: &str) -> Box<dyn TokenStream>;
}

impl Clone for Box<dyn TextAnalyzerT> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

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

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: 'static + Send + Sync + Clone {
    /// Take a `Token` and transform it or return `None` if it's to be removed
    /// from the output stream.
    fn transform(&mut self, token: Token) -> Option<Token>;
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
/// let tokenizer = analyzer_builder(SimpleTokenizer)
///        .filter(RemoveLongFilter::limit(40))
///        .filter(LowerCaser::new()).build();
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
pub trait TokenStream: Iterator<Item = Token> {}

/// `Tokenizer` are in charge of splitting text into a stream of token
/// before indexing.
///
/// See the [module documentation](./index.html) for more detail.
pub trait Tokenizer: 'static + Send + Sync + Clone {
    /// An iteratable type is returned.
    type Iter: TokenStream;
    /// Creates a token stream for a given `str`.
    fn token_stream(&self, text: &str) -> Self::Iter;
    /// Tokenize an array`&str`
    ///
    /// The resulting `BoxTokenStream` is equivalent to what would be obtained if the &str were
    /// one concatenated `&str`, with an artificial position gap of `2` between the different fields
    /// to prevent accidental `PhraseQuery` to match accross two terms.
    fn token_stream_texts<'a>(&'a self, texts: &'a [&str]) -> Box<dyn TokenStream + 'a> {
        let streams_with_offsets = texts.iter().scan(0, move |total_offset, &text| {
            let temp = *total_offset;
            *total_offset += text.len();
            Some((self.token_stream(text), temp))
        });
        Box::new(TokenStreamChain::new(streams_with_offsets))
    }
}

/// `TextAnalyzer` tokenizes an input text into tokens and modifies the resulting `TokenStream`.
///
/// It simply wraps a `Tokenizer` and a list of `TokenFilter` that are applied sequentially.
#[derive(Clone, Debug, Default)]
pub struct TextAnalyzer<T>(T);

impl<T: Tokenizer> From<T> for TextAnalyzer<T> {
    fn from(src: T) -> TextAnalyzer<T> {
        TextAnalyzer(src)
    }
}

impl<T: Tokenizer> TextAnalyzerClone for TextAnalyzer<T> {
    fn box_clone(&self) -> Box<dyn TextAnalyzerT> {
        Box::new(TextAnalyzer(self.0.clone()))
    }
}

impl<T: Tokenizer> TextAnalyzerT for TextAnalyzer<T> {
    fn token_stream(&self, text: &str) -> Box<dyn TokenStream> {
        Box::new(self.0.token_stream(text))
    }
}

/// Identity `TokenFilter`
#[derive(Clone, Debug, Default)]
pub struct Identity;

impl TokenFilter for Identity {
    fn transform(&mut self, token: Token) -> Option<Token> {
        Some(token)
    }
}

/// `Filter` is a wrapper around a `TokenStream` and a `TokenFilter` which modifies the `TokenStream`.
#[derive(Clone, Default, Debug)]
pub struct Filter<I, F> {
    iter: I,
    f: F,
}

impl<I, F> Iterator for Filter<I, F>
where
    I: TokenStream,
    F: TokenFilter,
{
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        while let Some(token) = self.iter.next() {
            if let Some(tok) = self.f.transform(token) {
                return Some(tok);
            }
        }
        None
    }
}

impl<I, F> TokenStream for Filter<I, F>
where
    I: TokenStream,
    F: TokenFilter,
{
}

#[derive(Clone, Debug, Default)]
pub struct AnalyzerBuilder<T, F> {
    tokenizer: T,
    f: F,
}

/// Construct an `AnalyzerBuilder` on which to apply `TokenFilter`.
pub fn analyzer_builder<T: Tokenizer>(tokenizer: T) -> AnalyzerBuilder<T, Identity> {
    AnalyzerBuilder {
        tokenizer,
        f: Identity,
    }
}

impl<T, F> AnalyzerBuilder<T, F>
where
    T: Tokenizer,
    F: TokenFilter,
{
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
    /// let en_stem = analyzer_builder(SimpleTokenizer)
    ///     .filter(RemoveLongFilter::limit(40))
    ///     .filter(LowerCaser::new())
    ///     .filter(Stemmer::default()).build();
    /// ```
    ///
    pub fn filter<G: TokenFilter>(self, f: G) -> AnalyzerBuilder<AnalyzerBuilder<T, F>, G> {
        AnalyzerBuilder { tokenizer: self, f }
    }
    /// Finalize the build process.
    pub fn build(self) -> TextAnalyzer<AnalyzerBuilder<T, F>> {
        TextAnalyzer(self)
    }
}

impl<T: Tokenizer, F: TokenFilter> Tokenizer for AnalyzerBuilder<T, F> {
    type Iter = Filter<T::Iter, F>;
    fn token_stream(&self, text: &str) -> Self::Iter {
        Filter {
            iter: self.tokenizer.token_stream(text),
            f: self.f.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tokenizer::SimpleTokenizer;

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

    #[test]
    fn text_analyzer() {
        let mut stream = SimpleTokenizer.token_stream("tokenizer hello world");
        dbg!(stream.next());
        dbg!(stream.next());
        dbg!(stream.next());
        dbg!(stream.next());
        dbg!(stream.next());
        dbg!(stream.next());
    }
}
