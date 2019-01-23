#![cfg_attr(feature = "cargo-clippy", allow(clippy::new_without_default))]

use super::{Token, TokenFilter, TokenStream};
use rust_stemmers::{self, Algorithm};
use std::sync::Arc;

/// Available stemmer languages.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
#[allow(missing_docs)]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Hungarian,
    Italian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish
}

impl Language {
    fn algorithm(&self) -> Algorithm {
        use self::Language::*;

        match self {
            Arabic => Algorithm::Arabic,
            Danish => Algorithm::Danish,
            Dutch => Algorithm::Dutch,
            English => Algorithm::English,
            Finnish => Algorithm::Finnish,
            French => Algorithm::French,
            German => Algorithm::German,
            Hungarian => Algorithm::Hungarian,
            Italian => Algorithm::Italian,
            Portuguese => Algorithm::Portuguese,
            Romanian => Algorithm::Romanian,
            Russian => Algorithm::Russian,
            Spanish => Algorithm::Spanish,
            Swedish => Algorithm::Swedish,
            Tamil => Algorithm::Tamil,
            Turkish => Algorithm::Turkish
        }
    }
}

/// `Stemmer` token filter. Several languages are supported, see `Language` for the available
/// languages.
/// Tokens are expected to be lowercased beforehand.
#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: Arc<Algorithm>,
}

impl Stemmer {
    /// Creates a new Stemmer `TokenFilter` for a given language algorithm.
    pub fn new(language: Language) -> Stemmer {
        Stemmer {
            stemmer_algorithm: Arc::new(language.algorithm()),
        }
    }
}

impl Default for Stemmer {
    /// Creates a new Stemmer `TokenFilter` for English.
    fn default() -> Self {
        Stemmer::new(Language::English)
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
