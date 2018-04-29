use super::{Token, TokenStream, Tokenizer};

///Tokenize the text by splitting words into ngrams of the given size
#[derive(Clone)]
pub struct NgramTokenizer {
  pub min_gram: usize,
  pub max_gram: usize,
  pub edges_only: bool,
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
    // best way to configure min/max gram + edges only
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
  // Some(1), Some(2), None, Some(1), Some(2), None
  fn cycle(&mut self) -> Option<usize> {
    if self.gram_size <= self.max_gram {
      // this seems awkward
      let r = Some(self.gram_size);
      self.gram_size += 1;
      r
    } else {
      self.gram_size = self.min_gram;
      None
    }
  }
}

impl<'a> TokenStream for NgramTokenStream<'a> {
  fn advance(&mut self) -> bool {
    // clear out working token text
    self.token.text.clear();

    loop {
      if self.location < self.text_length - 1 && (self.location + self.min_gram) <= self.text_length
      {
        // set the position of this token as the position of the last token + 1
        self.token.position = self.token.position.wrapping_add(1);

        // cycle through 1,2 (min...max)
        match self.cycle() {
          Some(size) => {
            let offset_from = self.location;
            let offset_to = offset_from + size;

            // println!(
            //   "location: {} - size: {} - offset_to:{}",
            //   self.location, size, offset_to
            // );

            self.token.offset_from = offset_from;
            self.token.offset_to = offset_to;
            self.token.text.push_str(&self.text[offset_from..offset_to]);
          }
          None => {
            //println!("next location");
            // move us down the chain of letters
            self.location = self.location + 1;
            continue;
          }
        }

        return true;
      } else {
        return false;
      }
    }
  }

  fn token(&self) -> &Token {
    &self.token
  }

  fn token_mut(&mut self) -> &mut Token {
    &mut self.token
  }
}
