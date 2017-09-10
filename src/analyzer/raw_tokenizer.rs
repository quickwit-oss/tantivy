use super::{Token, Analyzer, TokenStream};

#[derive(Clone)]
pub struct RawTokenizer;

pub struct RawTokenStream {
    token: Token,
    has_token: bool,
}

impl<'a> Analyzer<'a> for RawTokenizer {
    type TokenStreamImpl = RawTokenStream;

    fn token_stream(&mut self, text: &'a str) -> Self::TokenStreamImpl {
        let token = Token {
            offset_from: 0,
            offset_to: text.len(),
            position: 0,
            term: text.to_string()
        };
        RawTokenStream {
            token: token,
            has_token: true,
        }
    }
}

impl TokenStream for RawTokenStream {
    fn advance(&mut self) -> bool {
        if self.has_token {
            self.has_token = false;
            true
        }
        else {
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
