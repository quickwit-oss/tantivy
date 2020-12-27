use super::{Token, TokenStream, Tokenizer};
use std::str::CharIndices;

/// Tokenize the text by splitting on whitespaces and punctuation.
#[derive(Clone, Debug)]
pub struct SimpleTokenizer;

#[derive(Clone, Debug)]
pub struct SimpleTokenizerStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: Token,
}

impl<'a> Tokenizer<'a> for SimpleTokenizer {
    type Iter = SimpleTokenizerStream<'a>;
    fn token_stream(&self, text: &'a str) -> Self::Iter {
        SimpleTokenizerStream {
            text,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

impl<'a> SimpleTokenizerStream<'a> {
    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)| !c.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }
}

impl<'a> Iterator for SimpleTokenizerStream<'a> {
    type Item = Token;
    fn next(&mut self) -> Option<Self::Item> {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return Some(self.token.clone());
            }
        }
        None
    }
}

impl<'a> TokenStream for SimpleTokenizerStream<'a> {}
