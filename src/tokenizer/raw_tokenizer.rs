use super::{Token, TokenStream, Tokenizer};

/// For each value of the field, emit a single unprocessed token.
#[derive(Clone)]
pub struct RawTokenizer;

pub struct RawTokenStream {
    token: Token,
    has_token: bool,
}

impl Tokenizer for RawTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> Box<dyn TokenStream + 'a> {
        let token = Token {
            offset_from: 0,
            offset_to: text.len(),
            position: 0,
            text: text.to_string(),
            position_length: 1,
        };
        Box::new(RawTokenStream {
            token,
            has_token: true,
        })
    }
}

impl TokenStream for RawTokenStream {
    fn advance(&mut self) -> bool {
        let result = self.has_token;
        self.has_token = false;
        result
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}
