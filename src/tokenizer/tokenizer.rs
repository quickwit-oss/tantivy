/// The tokenizer module contains all of the tools used to process
/// text in `tantivy`.

use std::borrow::{Borrow, BorrowMut};
use tokenizer::TokenStreamChain;

/// Token 
///
///
///
/// # Example
///
/// ```
/// extern crate tantivy;
/// use tantivy::tokenizer::*;
///
/// # fn main() {
/// let mut tokenizer = SimpleTokenizer
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
/// #
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
}

impl Default for Token {
    fn default() -> Token {
        Token {
            offset_from: 0,
            offset_to: 0,
            position: usize::max_value(),
            text: String::new(),
        }
    }
}


// Warning! TODO may change once associated type constructor
// land in nightly.


pub trait Tokenizer<'a>: Sized + Clone {
    type TokenStreamImpl: TokenStream;

    fn token_stream(&mut self, text: &'a str) -> Self::TokenStreamImpl;

    fn filter<NewFilter>(self, new_filter: NewFilter) -> ChainTokenizer<NewFilter, Self>
        where NewFilter: TokenFilterFactory<<Self as Tokenizer<'a>>::TokenStreamImpl>
    {
        ChainTokenizer {
            head: new_filter,
            tail: self,
        }
    }
}

pub trait BoxedTokenizer: Send + Sync {
    fn token_stream<'a>(&mut self, text: &'a str) -> Box<TokenStream + 'a>;
    fn token_stream_texts<'b>(&mut self, texts: &'b [&'b str]) -> Box<TokenStream + 'b>;
    fn boxed_clone(&self) -> Box<BoxedTokenizer>;
}

#[derive(Clone)]
struct BoxableTokenizer<A>(A) where A: for <'a> Tokenizer<'a> + Send + Sync;

impl<A> BoxedTokenizer for BoxableTokenizer<A> where A: 'static + Send + Sync + for <'a> Tokenizer<'a> {
    fn token_stream<'a>(&mut self, text: &'a str) -> Box<TokenStream + 'a> {
        box self.0.token_stream(text)
    }

    fn token_stream_texts<'b>(&mut self, texts: &'b [&'b str]) -> Box<TokenStream + 'b> {
        assert!(texts.len() > 0);
        if texts.len() == 1 {
            box self.0.token_stream(texts[0])
        }
        else {
            let mut offsets = vec!();
            let mut total_offset = 0;
            for text in texts {
                offsets.push(total_offset);
                total_offset += text.len();
            }
            let token_streams: Vec<_> = texts
                .iter()
                .map(|text| {
                    self.0.token_stream(text)
                })
                .collect();
            box TokenStreamChain::new(offsets, token_streams)
        }
    }

    fn boxed_clone(&self) -> Box<BoxedTokenizer> {
        box self.clone()
    }
}

pub fn box_tokenizer<A>(a: A) -> Box<BoxedTokenizer>
    where A: 'static + Send + Sync + for <'a> Tokenizer<'a> {
    box BoxableTokenizer(a)
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


pub trait TokenStream {
    fn advance(&mut self) -> bool;

    fn token(&self) -> &Token;

    fn token_mut(&mut self) -> &mut Token;

    fn next(&mut self) -> Option<&Token> {
        if self.advance() {
            Some(self.token())
        } else {
            None
        }
    }

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
    where HeadTokenFilterFactory: TokenFilterFactory<TailTokenizer::TokenStreamImpl>,
          TailTokenizer: Tokenizer<'a>
{
    type TokenStreamImpl = HeadTokenFilterFactory::ResultTokenStream;

    fn token_stream(&mut self, text: &'a str) -> Self::TokenStreamImpl {
        let tail_token_stream = self.tail.token_stream(text );
        self.head.transform(tail_token_stream)
    }
}


pub trait TokenFilterFactory<TailTokenStream: TokenStream>: Clone {
    type ResultTokenStream: TokenStream;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream;
}
