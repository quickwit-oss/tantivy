use super::{Token, TokenStream, Tokenizer};

/// For each value of the field, emit a single unprocessed token.
#[derive(Clone, Debug)]
pub struct RawTokenizer;

#[derive(Clone, Debug)]
pub struct RawTokenStream {
    token: Token,
    has_token: bool,
}

impl Tokenizer for RawTokenizer {
    type Iter = RawTokenStream;
    fn token_stream(&self, text: &str) -> Self::Iter {
        let token = Token {
            offset_from: 0,
            offset_to: text.len(),
            position: 0,
            text: text.to_string(),
            position_length: 1,
        };
        RawTokenStream {
            token,
            has_token: true,
        }
    }
}

impl Iterator for RawTokenStream {
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        if self.has_token {
            self.has_token = false;
            Some(self.token.clone())
        } else {
            None
        }
    }
}

impl TokenStream for RawTokenStream {}
