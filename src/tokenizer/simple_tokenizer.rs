use super::{Token, TokenStream, Tokenizer};
use std::str::CharIndices;

/// Tokenize the text by splitting on whitespaces and punctuation.
#[derive(Clone)]
pub struct SimpleTokenizer;

pub struct SimpleTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: Token,
}

impl<'a> Tokenizer<'a> for SimpleTokenizer {
    type TokenStreamImpl = SimpleTokenStream<'a>;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        SimpleTokenStream {
            text,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

impl<'a> SimpleTokenStream<'a> {
    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)| !c.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }
}

impl<'a> TokenStream for SimpleTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        loop {
            match self.chars.next() {
                Some((offset_from, c)) => {
                    if c.is_alphanumeric() {
                        let offset_to = self.search_token_end();
                        self.token.offset_from = offset_from;
                        self.token.offset_to = offset_to;
                        self.token.text.push_str(&self.text[offset_from..offset_to]);
                        return true;
                    }
                }
                None => {
                    return false;
                }
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
