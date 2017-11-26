use tokenizer::{TokenStream, Token};

pub struct TokenStreamChain<TTokenStream: TokenStream> {
    offsets: Vec<usize>,
    token_streams: Vec<TTokenStream>,
    position_shift: usize,
    stream_idx: usize,
    token: Token,
}


impl<'a, TTokenStream> TokenStreamChain<TTokenStream>
where
    TTokenStream: TokenStream,
{
    pub fn new(
        offsets: Vec<usize>,
        token_streams: Vec<TTokenStream>,
    ) -> TokenStreamChain<TTokenStream> {
        TokenStreamChain {
            offsets: offsets,
            stream_idx: 0,
            token_streams: token_streams,
            position_shift: 0,
            token: Token::default(),
        }
    }
}

impl<'a, TTokenStream> TokenStream for TokenStreamChain<TTokenStream>
where
    TTokenStream: TokenStream,
{
    fn advance(&mut self) -> bool {
        while self.stream_idx < self.token_streams.len() {
            let token_stream = &mut self.token_streams[self.stream_idx];
            if token_stream.advance() {
                let token = token_stream.token();
                let offset_offset = self.offsets[self.stream_idx];
                self.token.offset_from = token.offset_from + offset_offset;
                self.token.offset_from = token.offset_from + offset_offset;
                self.token.position = token.position + self.position_shift;
                self.token.text.clear();
                self.token.text.push_str(token.text.as_str());
                return true;
            } else {
                self.stream_idx += 1;
                self.position_shift = self.token.position + 2;
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
