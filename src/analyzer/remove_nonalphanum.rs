use super::{TokenFilterFactory, TokenStream, Token};


pub struct RemoveNonAlphaFilter;

impl<TailTokenStream> RemoveNonAlphaFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn predicate(&self, token: &Token) -> bool {
        for c in token.term.chars() {
            if !c.is_alphanumeric() {
                return false;
            }
        }
        true
    }
}


impl<TailTokenStream> TokenFilterFactory<TailTokenStream> for RemoveNonAlphaFilter
    where TailTokenStream: TokenStream
{
    type ResultTokenStream = RemoveNonAlphaFilterStream<TailTokenStream>;

    fn transform(&self, tail: TailTokenStream) -> Self::ResultTokenStream {
        RemoveNonAlphaFilterStream { tail: tail }
    }
}

pub struct RemoveNonAlphaFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    tail: TailTokenStream,
}

impl<TailTokenStream> TokenStream for RemoveNonAlphaFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

    fn advance(&mut self) -> bool {
        loop {
            if self.tail.advance() {
                if self.predicate(self.tail.token()) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }
}
