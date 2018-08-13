/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use std::borrow::{Borrow, BorrowMut};
use tokenizer::TokenStreamChain;

/// Token
#[derive(Debug, Clone)]
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

/// `Tokenizer` are in charge of splitting text into a stream of token
/// before indexing.
///
/// See the [module documentation](./index.html) for more detail.
///
/// # Warning
///
/// This API may change to use associated types.
pub trait Tokenizer<'a>: Sized + Clone {
    /// Type associated to the resulting tokenstream tokenstream.
    type TokenStreamImpl: TokenStream;

    /// Creates a token stream for a given `str`.
    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl;

    /// Appends a token filter to the current tokenizer.
    ///
    /// The method consumes the current `TokenStream` and returns a
    /// new one.
    ///
    /// # Example
    ///
    /// ```rust
    /// # extern crate tantivy;
    ///
    /// use tantivy::tokenizer::*;
    ///
    /// # fn main() {
    /// let en_stem = SimpleTokenizer
    ///     .filter(RemoveLongFilter::limit(40))
    ///     .filter(LowerCaser)
    ///     .filter(Stemmer::new());
    /// # }
    /// ```
    ///
    fn filter<NewFilter>(self, new_filter: NewFilter) -> ChainTokenizer<NewFilter, Self>
    where
        NewFilter: TokenFilter<<Self as Tokenizer<'a>>::TokenStreamImpl>,
    {
        ChainTokenizer {
            head: new_filter,
            tail: self,
        }
    }
}

/// A boxed tokenizer
pub trait BoxedTokenizer: Send + Sync {
    /// Tokenize a `&str`
    fn token_stream<'a>(&self, text: &'a str) -> Box<TokenStream + 'a>;

    /// Tokenize an array`&str`
    ///
    /// The resulting `TokenStream` is equivalent to what would be obtained if the &str were
    /// one concatenated `&str`, with an artificial position gap of `2` between the different fields
    /// to prevent accidental `PhraseQuery` to match accross two terms.
    fn token_stream_texts<'b>(&self, texts: &'b [&'b str]) -> Box<TokenStream + 'b>;

    /// Return a boxed clone of the tokenizer
    fn boxed_clone(&self) -> Box<BoxedTokenizer>;
}

#[derive(Clone)]
struct BoxableTokenizer<A>(A)
where
    A: for<'a> Tokenizer<'a> + Send + Sync;

impl<A> BoxedTokenizer for BoxableTokenizer<A>
where
    A: 'static + Send + Sync + for<'a> Tokenizer<'a>,
{
    fn token_stream<'a>(&self, text: &'a str) -> Box<TokenStream + 'a> {
        Box::new(self.0.token_stream(text))
    }

    fn token_stream_texts<'b>(&self, texts: &'b [&'b str]) -> Box<TokenStream + 'b> {
        assert!(!texts.is_empty());
        if texts.len() == 1 {
            Box::new(self.0.token_stream(texts[0]))
        } else {
            let mut offsets = vec![];
            let mut total_offset = 0;
            for &text in texts {
                offsets.push(total_offset);
                total_offset += text.len();
            }
            let token_streams: Vec<_> =
                texts.iter().map(|text| self.0.token_stream(text)).collect();
            Box::new(TokenStreamChain::new(offsets, token_streams))
        }
    }

    fn boxed_clone(&self) -> Box<BoxedTokenizer> {
        Box::new(self.clone())
    }
}

pub(crate) fn box_tokenizer<A>(a: A) -> Box<BoxedTokenizer>
where
    A: 'static + Send + Sync + for<'a> Tokenizer<'a>,
{
    Box::new(BoxableTokenizer(a))
}

impl<'b> TokenStream for Box<TokenStream + 'b> {
    fn advance(&mut self) -> bool {
        let token_stream: &mut TokenStream = self.borrow_mut();
        token_stream.advance()
    }

    fn token(&self) -> &Token {
        let token_stream: &TokenStream = self.borrow();
        token_stream.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        let token_stream: &mut TokenStream = self.borrow_mut();
        token_stream.token_mut()
    }
}

/// `TokenStream` is the result of the tokenization.
///
/// It consists consumable stream of `Token`s.
///
/// # Example
///
/// ```
/// extern crate tantivy;
/// use tantivy::tokenizer::*;
///
/// # fn main() {
/// let tokenizer = SimpleTokenizer
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
/// # }
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
    /// # extern crate tantivy;
    /// # use tantivy::tokenizer::*;
    /// #
    /// # fn main() {
    /// # let tokenizer = SimpleTokenizer
    /// #       .filter(RemoveLongFilter::limit(40))
    /// #       .filter(LowerCaser);
    /// let mut token_stream = tokenizer.token_stream("Hello, happy tax payer");
    /// while let Some(token) = token_stream.next() {
    ///     println!("Token {:?}", token.text);
    /// }
    /// # }
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
    fn process(&mut self, sink: &mut FnMut(&Token)) -> u32 {
        let mut num_tokens_pushed = 0u32;
        while self.advance() {
            sink(self.token());
            num_tokens_pushed += 1u32;
        }
        num_tokens_pushed
    }
}

#[derive(Clone)]
pub struct ChainTokenizer<HeadTokenFilterFactory, TailTokenizer> {
    head: HeadTokenFilterFactory,
    tail: TailTokenizer,
}

impl<'a, HeadTokenFilterFactory, TailTokenizer> Tokenizer<'a>
    for ChainTokenizer<HeadTokenFilterFactory, TailTokenizer>
where
    HeadTokenFilterFactory: TokenFilter<TailTokenizer::TokenStreamImpl>,
    TailTokenizer: Tokenizer<'a>,
{
    type TokenStreamImpl = HeadTokenFilterFactory::ResultTokenStream;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        let tail_token_stream = self.tail.token_stream(text);
        self.head.transform(tail_token_stream)
    }
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter<TailTokenStream: TokenStream>: Clone {
    /// The resulting `TokenStream` type.
    type ResultTokenStream: TokenStream;

    /// Wraps a token stream and returns the modified one.
    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream;
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
            position_length: 1
        };
        let t2 = t1.clone();

        assert_eq!(t1.position, t2.position);
        assert_eq!(t1.offset_from, t2.offset_from);
        assert_eq!(t1.offset_to, t2.offset_to);
        assert_eq!(t1.text, t2.text);
    }
}
