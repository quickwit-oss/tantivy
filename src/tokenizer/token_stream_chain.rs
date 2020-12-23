use crate::tokenizer::{Token, TokenStream};

const POSITION_GAP: usize = 2;

pub(crate) struct Chain<'a, I> {
    streams_with_offsets: Vec<(I, usize)>,
    stream_idx: usize,
    position_shift: usize,
}

impl<'a, I> Chain<'a, I>
where
    I: Iterator<Item = Token>,
{
    pub fn new(streams_with_offsets: Vec<(I, usize)>) -> Chain<'a, I> {
        Chain {
            streams_with_offsets,
            stream_idx: 0,
            position_shift: 0,
        }
    }
}

impl<'a, I> Iterator for Chain<'a, I>
where
    I: Iterator<Item = Token>,
{
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        while self.stream_idx < self.streams_with_offsets.len() {
            let (ref mut token_stream, offset_offset) = self.streams_with_offsets[self.stream_idx];
            if let Some(token) = token_stream.next() {
                token.offset_from += offset_offset;
                token.offset_to += offset_offset;
                token.position += self.position_shift;
                return Some(token);
            } else {
                self.stream_idx += 1;
                self.position_shift = self.token.position.wrapping_add(POSITION_GAP);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::SimpleTokenizer;
    use super::*;

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
