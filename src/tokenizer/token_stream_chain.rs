use crate::tokenizer::{BoxTokenStream, Token, TokenStream};

const POSITION_GAP: usize = 2;

pub(crate) struct TokenStreamChain<'a> {
    streams_with_offsets: Vec<(BoxTokenStream<'a>, usize)>,
    position_shift: usize,
    stream_idx: usize,
    token: Token,
}

impl<'a> TokenStreamChain<'a> {
    pub fn new(streams_with_offsets: Vec<(BoxTokenStream<'a>, usize)>) -> TokenStreamChain<'a> {
        TokenStreamChain {
            streams_with_offsets,
            stream_idx: 0,
            position_shift: 0,
            token: Token::default(),
        }
    }
}

impl<'a> TokenStream for TokenStreamChain<'a> {
    fn advance(&mut self) -> bool {
        while self.stream_idx < self.streams_with_offsets.len() {
            let (ref mut token_stream, offset_offset) = self.streams_with_offsets[self.stream_idx];
            if token_stream.advance() {
                let token = token_stream.token();
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
            self.stream_idx <= self.streams_with_offsets.len(),
            "You called .token(), after the end of the token stream has been reached"
        );
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        assert!(
            self.stream_idx <= self.streams_with_offsets.len(),
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
            (SimpleTokenizer.token_stream(""), 0),
            (SimpleTokenizer.token_stream("hello world"), 0),
        ];
        let mut token_chain = TokenStreamChain::new(token_streams);

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
