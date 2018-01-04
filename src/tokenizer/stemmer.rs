use std::sync::Arc;
use super::{Token, TokenFilter, TokenStream};
use rust_stemmers::{self, Algorithm};

/// `Stemmer` token filter. Currently only English is supported.
/// Tokens are expected to be lowercased beforehands.
#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: Arc<Algorithm>,
}

impl Stemmer {
    /// Creates a new Stemmer `TokenFilter`.
    pub fn new() -> Stemmer {
        Stemmer {
            stemmer_algorithm: Arc::new(Algorithm::English),
        }
    }
}

impl<TailTokenStream> TokenFilter<TailTokenStream> for Stemmer
where
    TailTokenStream: TokenStream,
{
    type ResultTokenStream = StemmerTokenStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        let inner_stemmer = rust_stemmers::Stemmer::create(Algorithm::English);
        StemmerTokenStream::wrap(inner_stemmer, token_stream)
    }
}

pub struct StemmerTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    tail: TailTokenStream,
    stemmer: rust_stemmers::Stemmer,
}

impl<TailTokenStream> TokenStream for StemmerTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

    fn advance(&mut self) -> bool {
        if self.tail.advance() {
            // TODO remove allocation
            let stemmed_str: String = self.stemmer.stem(&self.token().text).into_owned();
            self.token_mut().text.clear();
            self.token_mut().text.push_str(&stemmed_str);
            true
        } else {
            false
        }
    }
}

impl<TailTokenStream> StemmerTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn wrap(
        stemmer: rust_stemmers::Stemmer,
        tail: TailTokenStream,
    ) -> StemmerTokenStream<TailTokenStream> {
        StemmerTokenStream { tail, stemmer }
    }
}
