use crate::tokenizer::{Token, TokenStream};

const POSITION_GAP: usize = 2;

pub(crate) struct TokenStreamChain<'a, I> {
    streams_with_offsets: I,
    position_shift: usize,
}

impl<'a, Out> TokenStreamChain<'a, Out> {
    pub fn new<In>(streams_with_offsets: Out) -> TokenStreamChain<'a, Out>
    where
        In: Iterator<Item = Token>,
        Out: Iterator<Item = In>,
    {
        TokenStreamChain {
            streams_with_offsets,
            position_shift: 0,
        }
    }
}

impl<'a, Out> TokenStream for TokenStreamChain<'a, Out> {}

impl<'a, In, Out> Iterator for TokenStreamChain<'a, Out>
where
    In: Iterator<Item = Token>,
    Out: Iterator<Item = In>,
{
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        while let Some((ref mut token_stream, offset_offset)) = self.streams_with_offsets.next() {
            if let Some(token) = token_stream.next() {
                token.offset_from += offset_offset;
                token.offset_to += offset_offset;
                token.position += self.position_shift;
                return Some(token);
            } else {
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
