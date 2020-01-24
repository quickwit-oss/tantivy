use crate::tokenizer::TokenStreamChain;
/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.
use std::borrow::{Borrow, BorrowMut};
use std::ops::Deref;

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

pub struct BoxedTokenizer(Box<dyn Tokenizer>);

impl Clone for BoxedTokenizer {
    fn clone(&self) -> Self {
        self.0.box_clone()
    }
}

impl Deref for BoxedTokenizer {
    type Target = dyn Tokenizer;

    fn deref(&self) -> &dyn Tokenizer {
        &*self.0
    }
}

impl<T: Tokenizer> From<T> for BoxedTokenizer {
    fn from(tokenizer: T) -> BoxedTokenizer {
        BoxedTokenizer(Box::new(tokenizer))
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
    fn token_stream<'a>(&self, text: &'a str) -> Box<dyn TokenStream + 'a>;

    fn token_stream_texts<'a>(&self, texts: &'a [&'a str]) -> Box<dyn TokenStream + 'a> {
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
            let token_streams: Vec<Box<dyn TokenStream + 'a>> = texts
                .iter()
                .cloned()
                .map(|text| self.token_stream(text))
                .collect();
            Box::new(TokenStreamChain::new(offsets, token_streams))
        }
    }
}

pub trait TokenizerClone {
    fn box_clone(&self) -> BoxedTokenizer;
}

impl<T: Tokenizer + Clone> TokenizerClone for T {
    fn box_clone(&self) -> BoxedTokenizer {
        From::from(self.clone())
    }
}

pub trait TokenizerExt {
    fn filter(self, new_filter: Box<dyn TokenFilter>) -> BoxedTokenizer;
}

impl<T: Tokenizer> TokenizerExt for T {
    fn filter(self, token_filter: Box<dyn TokenFilter>) -> BoxedTokenizer {
        BoxedTokenizer::from(TokenizerWithFilter {
            tokenizer: From::from(self),
            token_filter,
        })
    }
}

impl TokenizerExt for BoxedTokenizer {
    fn filter(self, token_filter: Box<dyn TokenFilter>) -> BoxedTokenizer {
        BoxedTokenizer::from(TokenizerWithFilter {
            tokenizer: self,
            token_filter,
        })
    }
}

struct TokenizerWithFilter {
    tokenizer: BoxedTokenizer,
    token_filter: Box<dyn TokenFilter>,
}

impl Clone for TokenizerWithFilter {
    fn clone(&self) -> Self {
        TokenizerWithFilter {
            tokenizer: self.tokenizer.box_clone(),
            token_filter: self.token_filter.box_clone(),
        }
    }
}

impl Tokenizer for TokenizerWithFilter {
    fn token_stream<'a>(&self, text: &'a str) -> Box<dyn TokenStream + 'a> {
        let tokenized_text = self.tokenizer.token_stream(text);
        self.token_filter.transform(tokenized_text)
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

/// `TokenStream` is the result of the tokenization.
///
/// It consists consumable stream of `Token`s.
///
/// # Example
///
/// ```
/// use tantivy::tokenizer::*;
///
/// let tokenizer = SimpleTokenizer
///        .filter(Box::new(RemoveLongFilter::limit(40)))
///        .filter(Box::new((LowerCaser);
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
    /// let tokenizer = SimpleTokenizer
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
    fn box_clone(&self) -> Box<dyn TokenFilter>;
}

/// Trait for the pluggable components of `Tokenizer`s.
pub trait TokenFilter: 'static + Send + Sync + TokenFilterClone {
    /// Wraps a token stream and returns the modified one.
    fn transform<'a>(&self, token_stream: Box<dyn TokenStream + 'a>) -> Box<dyn TokenStream + 'a>;
}

impl<T: TokenFilter + Clone> TokenFilterClone for T {
    fn box_clone(&self) -> Box<dyn TokenFilter> {
        Box::new(self.clone())
    }
}

impl TokenFilterClone for Box<dyn TokenFilter> {
    fn box_clone(&self) -> Box<dyn TokenFilter> {
        self.as_ref().box_clone()
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
