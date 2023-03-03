use crate::tokenizer::{Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub(crate) struct EmptyTokenizer;

impl Tokenizer for EmptyTokenizer {
    type TokenStream<'a> = EmptyTokenStream;
    fn token_stream<'a>(&self, _text: &'a str) -> EmptyTokenStream {
        EmptyTokenStream::default()
    }
}

#[derive(Default)]
pub struct EmptyTokenStream {
    token: Token,
}

impl TokenStream for EmptyTokenStream {
    fn advance(&mut self) -> bool {
        false
    }

    fn token(&self) -> &super::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut super::Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::{TokenStream, Tokenizer};

    #[test]
    fn test_empty_tokenizer() {
        let tokenizer = super::EmptyTokenizer;
        let mut empty = tokenizer.token_stream("whatever string");
        assert!(!empty.advance());
    }
}
