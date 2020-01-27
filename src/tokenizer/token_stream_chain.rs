use crate::tokenizer::{BoxTokenStream, Token, TokenStream};
use std::ops::DerefMut;

const POSITION_GAP: usize = 2;

pub(crate) struct TokenStreamChain<'a> {
    offsets: Vec<usize>,
    token_streams: Vec<BoxTokenStream<'a>>,
    position_shift: usize,
    stream_idx: usize,
    token: Token,
}

impl<'a> TokenStreamChain<'a> {
    pub fn new(
        offsets: Vec<usize>,
        token_streams: Vec<BoxTokenStream<'a>>,
    ) -> TokenStreamChain<'a> {
        TokenStreamChain {
            offsets,
            stream_idx: 0,
            token_streams,
            position_shift: 0,
            token: Token::default(),
        }
    }
}

impl<'a> TokenStream for TokenStreamChain<'a> {
    fn advance(&mut self) -> bool {
        while self.stream_idx < self.token_streams.len() {
            let token_stream = self.token_streams[self.stream_idx].deref_mut();
            if token_stream.advance() {
                let token = token_stream.token();
                let offset_offset = self.offsets[self.stream_idx];
                self.token.offset_from = token.offset_from + offset_offset;
                self.token.offset_to = token.offset_to + offset_offset;
                self.token.position = token.position + self.position_shift;
                self.token.text.clear();
                self.token.text.push_str(token.text.as_str());
                return true;
            } else {
                self.stream_idx += 1;
                self.position_shift = self.token.position.wrapping_add(POSITION_GAP);
            }
        }
        false
    }

    fn token(&self) -> &Token {
        assert!(
            self.stream_idx <= self.token_streams.len(),
            "You called .token(), after the end of the token stream has been reached"
        );
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        assert!(
            self.stream_idx <= self.token_streams.len(),
            "You called .token(), after the end of the token stream has been reached"
        );
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::super::{SimpleTokenizer, TokenStream, Tokenizer};
    use super::TokenStreamChain;
    use super::POSITION_GAP;

    #[test]
    fn test_chain_first_emits_no_tokens() {
        let token_streams = vec![
            SimpleTokenizer.token_stream(""),
            SimpleTokenizer.token_stream("hello world"),
        ];
        let mut token_chain = TokenStreamChain::new(vec![0, 0], token_streams);

        assert!(token_chain.advance());
        assert_eq!(token_chain.token().text, "hello");
        assert_eq!(token_chain.token().offset_from, 0);
        assert_eq!(token_chain.token().offset_to, 5);
        assert_eq!(token_chain.token().position, POSITION_GAP - 1);

        assert!(token_chain.advance());
        assert_eq!(token_chain.token().text, "world");
        assert_eq!(token_chain.token().offset_from, 6);
        assert_eq!(token_chain.token().offset_to, 11);
        assert_eq!(token_chain.token().position, POSITION_GAP);

        assert!(!token_chain.advance());
    }
}
