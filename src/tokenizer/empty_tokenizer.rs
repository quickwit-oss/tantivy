use crate::tokenizer::{BoxTokenStream, Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub(crate) struct EmptyTokenizer;

impl Tokenizer for EmptyTokenizer {
    fn token_stream<'a>(&self, _text: &'a str) -> BoxTokenStream<'a> {
        EmptyTokenStream::default().into()
    }
}

#[derive(Default)]
struct EmptyTokenStream {
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
    use crate::tokenizer::Tokenizer;

    #[test]
    fn test_empty_tokenizer() {
        let tokenizer = super::EmptyTokenizer;
        let mut empty = tokenizer.token_stream("whatever string");
        assert!(!empty.advance());
    }
}
