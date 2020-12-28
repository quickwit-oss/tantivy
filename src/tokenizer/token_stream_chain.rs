use crate::tokenizer::{Token, TokenStream, Tokenizer};

const POSITION_GAP: usize = 2;

pub(crate) struct TokenStreamChain<I> {
    streams_with_offsets: I,
    token: Token,
    position_shift: usize,
}

impl<'a, Out> TokenStreamChain<Out> {
    pub fn new<In>(streams_with_offsets: Out) -> TokenStreamChain<Out>
    where
        In: Iterator<Item = Token>,
        Out: Iterator<Item = (In, usize)>,
    {
        TokenStreamChain {
            streams_with_offsets,
            token: Token::default(),
            position_shift: 0,
        }
    }
}

impl<'a, In, Out: Iterator<Item = (In, usize)>> TokenStream for TokenStreamChain<Out> where
    In: Iterator<Item = Token>
{
}

impl<'a, In, Out> Iterator for TokenStreamChain<Out>
where
    In: Iterator<Item = Token>,
    Out: Iterator<Item = (In, usize)>,
{
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        while let Some((ref mut token_stream, offset_offset)) = self.streams_with_offsets.next() {
            if let Some(token) = token_stream.next() {
                self.token = token;
                self.token.offset_from += offset_offset;
                self.token.offset_to += offset_offset;
                self.token.position += self.position_shift;
                return Some(self.token.clone());
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
        let mut token_chain = TokenStreamChain::new(token_streams.into_iter());

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
