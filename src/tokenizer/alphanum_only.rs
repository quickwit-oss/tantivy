use super::{Token, TokenFilter, TokenStream};

/// `TokenFilter` that removes all tokens that contain non
/// ascii alphanumeric characters.
#[derive(Clone)]
pub struct AlphaNumOnlyFilter;

pub struct AlphaNumOnlyFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    tail: TailTokenStream,
}


impl<TailTokenStream> AlphaNumOnlyFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn predicate(&self, token: &Token) -> bool {
        token.text.chars().all(|c| c.is_ascii_alphanumeric())
    }

    fn wrap(
        tail: TailTokenStream,
    ) -> AlphaNumOnlyFilterStream<TailTokenStream> {
        AlphaNumOnlyFilterStream {
            tail
        }
    }
}


impl<TailTokenStream> TokenFilter<TailTokenStream> for AlphaNumOnlyFilter
    where
        TailTokenStream: TokenStream,
{
    type ResultTokenStream = AlphaNumOnlyFilterStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        AlphaNumOnlyFilterStream::wrap(token_stream)
    }
}

impl<TailTokenStream> TokenStream for AlphaNumOnlyFilterStream<TailTokenStream>
    where
        TailTokenStream: TokenStream
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
