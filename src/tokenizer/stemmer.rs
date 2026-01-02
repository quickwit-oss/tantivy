use std::borrow::Cow;
use std::mem;

use serde::{Deserialize, Serialize};

use super::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
enum StemmerAlgorithm {
    Rust(rust_stemmers::Algorithm),
    Tantivy(fn(&str) -> Cow<str>),
}

/// Available stemmer languages.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone, Hash)]
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
    Polish,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl Language {
    fn algorithm(self) -> StemmerAlgorithm {
        use self::Language::*;
        match self {
            Arabic => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Arabic),
            Danish => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Danish),
            Dutch => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Dutch),
            English => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::English),
            Finnish => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Finnish),
            French => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::French),
            German => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::German),
            Greek => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Greek),
            Hungarian => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Hungarian),
            Italian => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Italian),
            Norwegian => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Norwegian),
            Polish => StemmerAlgorithm::Tantivy(tantivy_stemmers::algorithms::polish_yarovoy),
            Portuguese => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Portuguese),
            Romanian => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Romanian),
            Russian => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Russian),
            Spanish => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Spanish),
            Swedish => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Swedish),
            Tamil => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Tamil),
            Turkish => StemmerAlgorithm::Rust(rust_stemmers::Algorithm::Turkish),
        }
    }
}

/// `Stemmer` token filter. Several languages are supported, see [`Language`] for the available
/// languages.
/// Tokens are expected to be lowercased beforehand.
#[derive(Clone)]
pub struct Stemmer {
    stemmer_algorithm: StemmerAlgorithm,
}

impl Stemmer {
    /// Creates a new `Stemmer` [`TokenFilter`] for a given language algorithm.
    pub fn new(language: Language) -> Stemmer {
        Stemmer {
            stemmer_algorithm: language.algorithm(),
        }
    }
}

impl Default for Stemmer {
    /// Creates a new `Stemmer` [`TokenFilter`] for [`Language::English`].
    fn default() -> Self {
        Stemmer::new(Language::English)
    }
}

impl TokenFilter for Stemmer {
    type Tokenizer<T: Tokenizer> = StemmerFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> StemmerFilter<T> {
        StemmerFilter {
            stemmer_algorithm: self.stemmer_algorithm,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct StemmerFilter<T> {
    stemmer_algorithm: StemmerAlgorithm,
    inner: T,
}

enum StemmerImpl {
    Rust(rust_stemmers::Stemmer),
    Tantivy(fn(&str) -> Cow<str>),
}

impl<T: Tokenizer> Tokenizer for StemmerFilter<T> {
    type TokenStream<'a> = StemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let stemmer = match self.stemmer_algorithm {
            StemmerAlgorithm::Rust(alg) => StemmerImpl::Rust(rust_stemmers::Stemmer::create(alg)),
            StemmerAlgorithm::Tantivy(f) => StemmerImpl::Tantivy(f),
        };
        StemmerTokenStream {
            tail: self.inner.token_stream(text),
            stemmer,
            buffer: String::new(),
        }
    }
}

pub struct StemmerTokenStream<T> {
    tail: T,
    stemmer: StemmerImpl,
    buffer: String,
}

impl<T: TokenStream> TokenStream for StemmerTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        let stemmed_str = match self.stemmer {
            StemmerImpl::Rust(ref s) => s.stem(&token.text),
            StemmerImpl::Tantivy(f) => f(&token.text),
        };
        match stemmed_str {
            Cow::Owned(stemmed_str) => token.text = stemmed_str,
            Cow::Borrowed(stemmed_str) => {
                self.buffer.clear();
                self.buffer.push_str(stemmed_str);
                mem::swap(&mut token.text, &mut self.buffer);
            }
        }
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}
