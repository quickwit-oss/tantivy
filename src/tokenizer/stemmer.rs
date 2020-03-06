use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;
use rust_stemmers::{self, Algorithm};
use serde::{Deserialize, Serialize};

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
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl Language {
    fn algorithm(self) -> Algorithm {
        use self::Language::*;
        match self {
            Arabic => Algorithm::Arabic,
            Danish => Algorithm::Danish,
            Dutch => Algorithm::Dutch,
            English => Algorithm::English,
            Finnish => Algorithm::Finnish,
            French => Algorithm::French,
            German => Algorithm::German,
            Greek => Algorithm::Greek,
            Hungarian => Algorithm::Hungarian,
            Italian => Algorithm::Italian,
            Norwegian => Algorithm::Norwegian,
            Portuguese => Algorithm::Portuguese,
            Romanian => Algorithm::Romanian,
            Russian => Algorithm::Russian,
            Spanish => Algorithm::Spanish,
            Swedish => Algorithm::Swedish,
            Tamil => Algorithm::Tamil,
            Turkish => Algorithm::Turkish,
        }
    }
}

/// `Stemmer` token filter. Several languages are supported, see `Language` for the available
/// languages.
/// Tokens are expected to be lowercased beforehand.
#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: Algorithm,
}

impl Stemmer {
    /// Creates a new Stemmer `TokenFilter` for a given language algorithm.
    pub fn new(language: Language) -> Stemmer {
        Stemmer {
            stemmer_algorithm: language.algorithm(),
        }
    }
}

impl Default for Stemmer {
    /// Creates a new Stemmer `TokenFilter` for English.
    fn default() -> Self {
        Stemmer::new(Language::English)
    }
}

impl TokenFilter for Stemmer {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        let inner_stemmer = rust_stemmers::Stemmer::create(self.stemmer_algorithm);
        BoxTokenStream::from(StemmerTokenStream {
            tail: token_stream,
            stemmer: inner_stemmer,
        })
    }
}

pub struct StemmerTokenStream<'a> {
    tail: BoxTokenStream<'a>,
    stemmer: rust_stemmers::Stemmer,
}

impl<'a> TokenStream for StemmerTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        // TODO remove allocation
        let stemmed_str: String = self.stemmer.stem(&self.token().text).into_owned();
        self.token_mut().text.clear();
        self.token_mut().text.push_str(&stemmed_str);
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}
