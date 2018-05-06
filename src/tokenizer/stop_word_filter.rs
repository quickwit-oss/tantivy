//! # Example
//! ```
//! extern crate tantivy;
//! use tantivy::tokenizer::*;
//!
//! # fn main() {
//! let tokenizer = SimpleTokenizer
//!   .filter(StopWordFilter::remove(vec!["the".to_string(),"is".to_string()]));
//!
//! let mut stream = tokenizer.token_stream("the fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! # }
//! ```
use super::{Token, TokenFilter, TokenStream};

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
  words: Vec<String>,
}

impl StopWordFilter {
  /// Creates a `StopWordFilter` given a list of words to remove
  pub fn remove(words: Vec<String>) -> StopWordFilter {
    StopWordFilter { words }
  }
}

pub struct StopWordFilterStream<TailTokenStream>
where
  TailTokenStream: TokenStream,
{
  words: Vec<String>,
  tail: TailTokenStream,
}

impl<TailTokenStream> TokenFilter<TailTokenStream> for StopWordFilter
where
  TailTokenStream: TokenStream,
{
  type ResultTokenStream = StopWordFilterStream<TailTokenStream>;

  fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
    StopWordFilterStream::wrap(self.words.clone(), token_stream)
  }
}

impl<TailTokenStream> StopWordFilterStream<TailTokenStream>
where
  TailTokenStream: TokenStream,
{
  fn predicate(&self, token: &Token) -> bool {
    !self.words.contains(&token.text)
  }

  fn wrap(words: Vec<String>, tail: TailTokenStream) -> StopWordFilterStream<TailTokenStream> {
    StopWordFilterStream { words, tail }
  }
}

impl<TailTokenStream> TokenStream for StopWordFilterStream<TailTokenStream>
where
  TailTokenStream: TokenStream,
{
  fn token(&self) -> &Token {
    self.tail.token()
  }

  fn token_mut(&mut self) -> &mut Token {
    self.tail.token_mut()
  }

  fn advance(&mut self) -> bool {
    while self.tail.advance() {
      if self.predicate(self.tail.token()) {
        return true;
      }
    }

    false
  }
}
