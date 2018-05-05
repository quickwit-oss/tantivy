use super::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting words into n-grams of the given size(s)
///
/// Example: `hello` would be tokenized as (min_gram: 2, max_gram: 3)
///
/// | Term     | he  | hel | el  | ell | ll  | llo | lo |
/// |----------|-----|-----|-----|-----|-----|-----|----|
/// | Position | 0   | 0   | 1   | 1   | 2   | 2   | 3  |
/// | Offsets  | 0,2 | 0,3 | 1,3 | 1,4 | 2,4 | 2,5 | 3,5|
///
/// # Example
///
/// ```
/// extern crate tantivy;
/// use tantivy::tokenizer::*;
///
/// fn assert_token(token: &Token, position: usize, text: &str, from: usize, to: usize) {
///   assert_eq!(
///       token.position, position,
///       "expected position {} but {:?}",
///       position, token
///   );
///   assert_eq!(token.text, text, "expected text {} but {:?}", text, token);
///   assert_eq!(
///       token.offset_from, from,
///       "expected offset_from {} but {:?}",
///       from, token
///   );
///   assert_eq!(
///       token.offset_to, to,
///       "expected offset_to {} but {:?}",
///       to, token
///   );
/// }
/// # fn main() {
/// let tokenizer_manager = TokenizerManager::default();
/// tokenizer_manager.register("ngram23", NgramTokenizer::new(2, 3, false));
/// let tokenizer = tokenizer_manager.get("ngram23").unwrap();
/// let mut tokens: Vec<Token> = vec![];
/// {
///     let mut add_token = |token: &Token| {
///         tokens.push(token.clone());
///     };
///     tokenizer.token_stream("hello").process(&mut add_token);
/// }
/// assert_eq!(tokens.len(), 7);
/// assert_token(&tokens[0], 0, "he", 0, 2);
/// assert_token(&tokens[1], 0, "hel", 0, 3);
/// assert_token(&tokens[2], 1, "el", 1, 3);
/// assert_token(&tokens[3], 1, "ell", 1, 4);
/// assert_token(&tokens[4], 2, "ll", 2, 4);
/// assert_token(&tokens[5], 2, "llo", 2, 5);
/// assert_token(&tokens[6], 3, "lo", 3, 5);
/// # }
/// ```
///
#[derive(Clone)]
pub struct NgramTokenizer {
  /// min size of the n-gram
  min_gram: usize,
  /// max size of the n-gram
  max_gram: usize,
  /// should we only process the leading edge of the input
  edges_only: bool,
}

impl NgramTokenizer {
  /// Configures a new Ngram tokenizer
  pub fn new(min_gram: usize, max_gram: usize, edges_only: bool) -> NgramTokenizer {
    assert!(min_gram > 0, "min_gram must be greater than 0");
    assert!(
      min_gram <= max_gram,
      "min_gram must not be greater than max_gram"
    );

    NgramTokenizer {
      min_gram,
      max_gram,
      edges_only,
    }
  }
}
pub struct NgramTokenStream<'a> {
  text: &'a str,
  location: usize,
  text_length: usize,
  token: Token,
  min_gram: usize,
  max_gram: usize,
  gram_size: usize,
  edges_only: bool,
}

impl<'a> Tokenizer<'a> for NgramTokenizer {
  type TokenStreamImpl = NgramTokenStream<'a>;

  fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
    NgramTokenStream {
      text,
      location: 0,
      text_length: text.len(),
      token: Token::default(),
      min_gram: self.min_gram,
      max_gram: self.max_gram,
      edges_only: self.edges_only,
      gram_size: self.min_gram,
    }
  }
}

impl<'a> NgramTokenStream<'a> {
  /// Get the next set of token options
  /// cycle through 1,2 (min..=max)
  /// returning None if processing should stop
  fn chomp(&mut self) -> Option<(usize, usize)> {
    // Have we exceeded the bounds of the text we are indexing?
    if self.gram_size > self.max_gram {
      if self.edges_only {
        return None;
      }

      // since we aren't just processing edges
      // we need to reset the gram size
      self.gram_size = self.min_gram;

      // and move down the chain of letters
      self.location += 1;
    }

    let result = if (self.location + self.gram_size) <= self.text_length {
      Some((self.location, self.gram_size))
    } else {
      None
    };

    // increase the gram size for the next pass
    self.gram_size += 1;

    result
  }
}

impl<'a> TokenStream for NgramTokenStream<'a> {
  fn advance(&mut self) -> bool {
    // clear out working token text
    self.token.text.clear();

    if let Some((location, size)) = self.chomp() {
      // this token's position is our current location
      self.token.position = location;
      let offset_from = location;
      let offset_to = offset_from + size;

      self.token.offset_from = offset_from;
      self.token.offset_to = offset_to;

      self.token.text.push_str(&self.text[offset_from..offset_to]);

      true
    } else {
      false
    }
  }

  fn token(&self) -> &Token {
    &self.token
  }

  fn token_mut(&mut self) -> &mut Token {
    &mut self.token
  }
}
