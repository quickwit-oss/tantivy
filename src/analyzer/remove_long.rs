use super::{TokenFilterFactory, TokenStream, Token};


pub struct RemoveLongFilter {
    length_limit: usize,
}

impl RemoveLongFilter {
    // the limit is in bytes of the UTF-8 representation.
    pub fn limit(length_limit: usize) -> RemoveLongFilter {
        RemoveLongFilter { length_limit: length_limit }
    }
}

impl<TailTokenStream> RemoveLongFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn predicate(&self, token: &Token) -> bool {
        token.term.len() < self.token_length_limit
    }

    fn wrap(token_length_limit: usize,
            tail: TailTokenStream)
            -> RemoveLongFilterStream<TailTokenStream> {
        RemoveLongFilterStream {
            token_length_limit: token_length_limit,
            tail: tail,
        }
    }
}


impl<TailTokenStream> TokenFilterFactory<TailTokenStream> for RemoveLongFilter
    where TailTokenStream: TokenStream
{
    type ResultTokenStream = RemoveLongFilterStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        RemoveLongFilterStream::wrap(self.length_limit, token_stream)
    }
}

pub struct RemoveLongFilterStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    token_length_limit: usize,
    tail: TailTokenStream,
}

impl<TailTokenStream> TokenStream for RemoveLongFilterStream<TailTokenStream>
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
