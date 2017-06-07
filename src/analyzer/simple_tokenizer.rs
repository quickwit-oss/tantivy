
use std::str::CharIndices;
use super::{Token, Analyzer, TokenStream};

pub struct SimpleTokenizer;

pub struct SimpleTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: Token,   
}

impl<'a> Analyzer<'a> for SimpleTokenizer {

    type TokenStreamImpl = SimpleTokenStream<'a>;

    fn analyze(&mut self, text: &'a str) -> Self::TokenStreamImpl {
        SimpleTokenStream {
            text: text,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}

impl<'a> SimpleTokenStream<'a> {
        
    fn token_limit(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)|  !c.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl<'a> TokenStream for SimpleTokenStream<'a> {

    fn advance(&mut self) -> bool {
        self.token.term.clear();
        self.token.position += 1;

        loop {
            match self.chars.next() {
                Some((offset_from, c)) => {
                    if c.is_alphanumeric() {
                        let offset_to = self.token_limit();
                        self.token.offset_from = offset_from;
                        self.token.offset_to = offset_to;
                        self.token.term.push_str(&self.text[offset_from..offset_to]);
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