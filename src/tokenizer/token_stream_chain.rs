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
impl DynTokenStreamChain {
    pub fn from_vec(streams_with_offsets: Vec<(Box<dyn TokenStream>, usize)>) -> impl TokenStream {
        DynTokenStreamChain {
            streams_with_offsets,
            idx: 0,
            token: Token::default(),
            position_shift: 0,
        }
    }
}

pub(crate) struct DynTokenStreamChain {
    streams_with_offsets: Vec<(Box<dyn TokenStream>, usize)>,
    idx: usize,
    token: Token,
    position_shift: usize,
}

impl<'a> TokenStream for DynTokenStreamChain {}

impl Iterator for DynTokenStreamChain {
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        if self.idx >= self.streams_with_offsets.len() {
            return None;
        };
        while self.idx < self.streams_with_offsets.len() {
            let (ref mut token_stream, offset_offset) = self.streams_with_offsets[self.idx];
            if let Some(token) = token_stream.next() {
                self.token = token;
                self.token.offset_from += offset_offset;
                self.token.offset_to += offset_offset;
                self.token.position += self.position_shift;
                return Some(self.token.clone());
            } else {
                self.idx += 1;
                self.position_shift = self.token.position.wrapping_add(POSITION_GAP);
            }
        }
        None
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
        let token = token_chain.next().unwrap();
        assert_eq!(token.text, "hello");
        assert_eq!(token.offset_from, 0);
        assert_eq!(token.offset_to, 5);
        assert_eq!(token.position, POSITION_GAP - 1);

        let token = token_chain.next().unwrap();
        assert_eq!(token.text, "world");
        assert_eq!(token.offset_from, 6);
        assert_eq!(token.offset_to, 11);
        assert_eq!(token.position, POSITION_GAP);

        assert!(token_chain.next().is_none());
    }
}
