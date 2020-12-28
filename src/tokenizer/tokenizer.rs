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
pub struct TextAnalyzer<T> {
    tokenizer: T,
    filters: Vec<Box<dyn TokenFilter>>,
}

pub trait TextAnalyzerT<'a>: 'static + Send + Sync + TextAnalyzerClone<'a> {
    fn token_stream(&self, text: &'a str) -> Box<dyn TokenStream + 'a>;
}

pub trait TextAnalyzerClone<'a> {
    fn box_clone(&self) -> Box<dyn TextAnalyzerT<'a>>;
}

impl<'a> Clone for Box<dyn TextAnalyzerT<'a>> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}
impl Clone for Box<dyn TokenFilter> {
    fn clone(&self) -> Self {
        (**self).box_clone()
    }
}

impl<'a, T: Clone + Tokenizer<'a>> TextAnalyzerClone<'a> for TextAnalyzer<T> {
    fn box_clone(&self) -> Box<dyn TextAnalyzerT<'a>> {
        Box::new(TextAnalyzer {
            tokenizer: self.tokenizer.clone(),
            filters: self.filters.clone(),
        })
    }
}

impl<'a, T: Tokenizer<'a>> TextAnalyzerT<'a> for TextAnalyzer<T> {
    fn token_stream(&self, text: &'a str) -> Box<dyn TokenStream + 'a> {
        let tokens = self.tokenizer.token_stream(text);
        Box::new(TextIter {
            tokens,
            // TODO: remove clone
            filters: self.filters.clone(),
        })
    }
}

impl<'a, T> TextAnalyzer<T>
where
    T: Tokenizer<'a>,
{
    /// Creates a new `TextAnalyzer` given a tokenizer and a vector of `Box<dyn TokenFilter>`.
    ///
    /// When creating a `TextAnalyzer` from a `Tokenizer` alone, prefer using
    /// `TextAnalyzer::from(tokenizer)`.
    pub fn new(tokenizer: T) -> TextAnalyzer<T> {
        TextAnalyzer {
            tokenizer,
            filters: vec![],
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
        self.filters.push(Box::new(token_filter));
        self
    }

    /// Tokenize an array`&str`
    ///
    /// The resulting `BoxTokenStream` is equivalent to what would be obtained if the &str were
    /// one concatenated `&str`, with an artificial position gap of `2` between the different fields
    /// to prevent accidental `PhraseQuery` to match accross two terms.

    /// Creates a token stream for a given `str`.
    pub fn token_stream(&self, text: &'a str) -> TextIter<T::Iter> {
        let tokens = self.tokenizer.token_stream(text);
        TextIter {
            tokens,
            // TODO: remove clone
            filters: self.filters.clone(),
        }
    }
}

struct TextIter<I> {
    tokens: I,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl<'a, I> Iterator for TextIter<I>
where
    I: Iterator<Item = Token>,
{
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        'outer: while let Some(mut token) = self.tokens.next() {
            for filter in self.filters.iter_mut() {
                if let Some(tok) = filter.transform(token) {
                    token = tok;
                    continue;
                };
                continue 'outer;
            }
        }
        None
    }
}

impl<I: Iterator<Item = Token>> TokenStream for TextIter<I> {}

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
    // TODO: make clone unnecessary
    fn token_stream(&self, text: &'a str) -> Self::Iter;
}

fn token_stream_texts<'a, T: Tokenizer<'a>>(
    tokenizer: &'a T,
    texts: &'a [&str],
) -> impl TokenStream + 'a {
    let streams_with_offsets = texts.iter().scan(0, move |total_offset, &text| {
        let temp = *total_offset;
        *total_offset += text.len();
        Some((tokenizer.token_stream(text), temp))
    });
    TokenStreamChain::new(streams_with_offsets)
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: 'static + Send + Sync + TokenFilterClone {
    fn transform(&mut self, token: Token) -> Option<Token>;
}

pub trait TokenFilterClone {
    fn box_clone(&self) -> Box<dyn TokenFilter>;
}

impl<T: TokenFilter + Clone> TokenFilterClone for T {
    fn box_clone(&self) -> Box<dyn TokenFilter> {
        Box::new(self.clone())
    }
}

pub trait TokenStream: Iterator<Item = Token> {
    fn process(&mut self, sink: &mut dyn FnMut(&Token)) -> u32 {
        let mut num_tokens_pushed = 0u32;
        while let Some(token) = self.next() {
            sink(&token);
            num_tokens_pushed += 1u32;
        }
        num_tokens_pushed
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
