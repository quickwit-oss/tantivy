use super::{TokenFilter, TokenStream, Token};

/// Token filter that lowercase terms.
#[derive(Clone)]
pub struct LowerCaser;

impl<TailTokenStream> TokenFilter<TailTokenStream> for LowerCaser
    where TailTokenStream: TokenStream
{
    type ResultTokenStream = LowerCaserTokenStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        LowerCaserTokenStream::wrap(token_stream)
    }
}

pub struct LowerCaserTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    tail: TailTokenStream,
}

impl<TailTokenStream> TokenStream for LowerCaserTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

    fn advance(&mut self) -> bool {
        if self.tail.advance() {
            self.tail.token_mut().text.make_ascii_lowercase();
            true
        } else {
            false
        }
    }
}

impl<TailTokenStream> LowerCaserTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn wrap(tail: TailTokenStream) -> LowerCaserTokenStream<TailTokenStream> {
        LowerCaserTokenStream { tail: tail }
    }
}
