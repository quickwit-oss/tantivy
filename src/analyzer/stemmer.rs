use std::sync::Arc;
use super::{TokenFilterFactory, TokenStream, Token};
use rust_stemmers::{self, Algorithm};

#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: Arc<Algorithm>,
}

impl Stemmer {
    pub fn new() -> Stemmer {
        Stemmer { stemmer_algorithm: Arc::new(Algorithm::English) }
    }
}

impl<TailTokenStream> TokenFilterFactory<TailTokenStream> for Stemmer
    where TailTokenStream: TokenStream
{
    type ResultTokenStream = StemmerTokenStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        let inner_stemmer = rust_stemmers::Stemmer::create(Algorithm::English);
        StemmerTokenStream::wrap(inner_stemmer, token_stream)
    }
}


pub struct StemmerTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    tail: TailTokenStream,
    stemmer: rust_stemmers::Stemmer,
}

impl<TailTokenStream> TokenStream for StemmerTokenStream<TailTokenStream>
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
            // TODO remove allocation
            let stemmed_str: String = self.stemmer.stem(&self.token().term).into_owned();
            self.token_mut().term.clear();
            self.token_mut().term.push_str(&stemmed_str);
            true
        } else {
            false
        }
    }
}

impl<TailTokenStream> StemmerTokenStream<TailTokenStream>
    where TailTokenStream: TokenStream
{
    fn wrap(stemmer: rust_stemmers::Stemmer,
            tail: TailTokenStream)
            -> StemmerTokenStream<TailTokenStream> {
        StemmerTokenStream {
            tail: tail,
            stemmer: stemmer,
        }
    }
}
