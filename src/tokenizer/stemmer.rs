use std::borrow::Cow;
use std::mem;

use frostem::Algorithm;
use serde::{Deserialize, Serialize};

use super::light_stemmer::LightAlgorithm;
use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// Available stemmer languages.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone, Hash)]
#[allow(missing_docs)]
pub enum Language {
    Arabic,
    Armenian,
    Basque,
    Bengali,
    Bulgarian,
    Catalan,
    Croatian,
    Czech,
    Danish,
    Dutch,
    English,
    Esperanto,
    Estonian,
    Finnish,
    French,
    German,
    Greek,
    Hindi,
    Hungarian,
    Indonesian,
    Irish,
    Italian,
    Latvian,
    Lithuanian,
    Nepali,
    Norwegian,
    Persian,
    Polish,
    Portuguese,
    Romanian,
    Russian,
    Serbian,
    Slovak,
    Sorani,
    Sotho,
    Spanish,
    Swedish,
    Tamil,
    Telugu,
    Turkish,
    Ukrainian,
}

impl Language {
    fn algorithm(self) -> StemmerAlgorithm {
        use self::Language::*;
        match self {
            Arabic => StemmerAlgorithm::Snowball(Algorithm::Arabic),
            Armenian => StemmerAlgorithm::Snowball(Algorithm::Armenian),
            Basque => StemmerAlgorithm::Snowball(Algorithm::Basque),
            Catalan => StemmerAlgorithm::Snowball(Algorithm::Catalan),
            // Snowball's Serbian algorithm is Ljubesic and Pandzic's "Stemmer for Croatian"
            // with a Cyrillic-to-Latin prelude, so it serves Croatian directly. See the header
            // of https://github.com/snowballstem/snowball/blob/master/algorithms/serbian.sbl
            Croatian => StemmerAlgorithm::Snowball(Algorithm::Serbian),
            Czech => StemmerAlgorithm::Snowball(Algorithm::Czech),
            Danish => StemmerAlgorithm::Snowball(Algorithm::Danish),
            Dutch => StemmerAlgorithm::Snowball(Algorithm::Dutch),
            English => StemmerAlgorithm::Snowball(Algorithm::English),
            Esperanto => StemmerAlgorithm::Snowball(Algorithm::Esperanto),
            Estonian => StemmerAlgorithm::Snowball(Algorithm::Estonian),
            Finnish => StemmerAlgorithm::Snowball(Algorithm::Finnish),
            French => StemmerAlgorithm::Snowball(Algorithm::French),
            German => StemmerAlgorithm::Snowball(Algorithm::German),
            Greek => StemmerAlgorithm::Snowball(Algorithm::Greek),
            Hindi => StemmerAlgorithm::Snowball(Algorithm::Hindi),
            Hungarian => StemmerAlgorithm::Snowball(Algorithm::Hungarian),
            Indonesian => StemmerAlgorithm::Snowball(Algorithm::Indonesian),
            Irish => StemmerAlgorithm::Snowball(Algorithm::Irish),
            Italian => StemmerAlgorithm::Snowball(Algorithm::Italian),
            Lithuanian => StemmerAlgorithm::Snowball(Algorithm::Lithuanian),
            Nepali => StemmerAlgorithm::Snowball(Algorithm::Nepali),
            Norwegian => StemmerAlgorithm::Snowball(Algorithm::Norwegian),
            Persian => StemmerAlgorithm::Snowball(Algorithm::Persian),
            Polish => StemmerAlgorithm::Snowball(Algorithm::Polish),
            Portuguese => StemmerAlgorithm::Snowball(Algorithm::Portuguese),
            Romanian => StemmerAlgorithm::Snowball(Algorithm::Romanian),
            Russian => StemmerAlgorithm::Snowball(Algorithm::Russian),
            Serbian => StemmerAlgorithm::Snowball(Algorithm::Serbian),
            Sotho => StemmerAlgorithm::Snowball(Algorithm::Sesotho),
            Spanish => StemmerAlgorithm::Snowball(Algorithm::Spanish),
            Swedish => StemmerAlgorithm::Snowball(Algorithm::Swedish),
            Tamil => StemmerAlgorithm::Snowball(Algorithm::Tamil),
            Turkish => StemmerAlgorithm::Snowball(Algorithm::Turkish),
            Ukrainian => StemmerAlgorithm::Snowball(Algorithm::Ukrainian),

            // Snowball has no algorithm for these; see [`super::light_stemmer`].
            Bengali => StemmerAlgorithm::Light(LightAlgorithm::Bengali),
            Bulgarian => StemmerAlgorithm::Light(LightAlgorithm::Bulgarian),
            Latvian => StemmerAlgorithm::Light(LightAlgorithm::Latvian),
            Slovak => StemmerAlgorithm::Light(LightAlgorithm::Slovak),
            Sorani => StemmerAlgorithm::Light(LightAlgorithm::Sorani),
            Telugu => StemmerAlgorithm::Light(LightAlgorithm::Telugu),
        }
    }
}

#[cfg(test)]
impl Language {
    /// Every language, so tests can iterate them.
    ///
    /// Kept complete by `test_all_languages_are_listed`.
    pub(crate) const ALL: &'static [Language] = &[
        Language::Arabic,
        Language::Armenian,
        Language::Basque,
        Language::Bengali,
        Language::Bulgarian,
        Language::Catalan,
        Language::Croatian,
        Language::Czech,
        Language::Danish,
        Language::Dutch,
        Language::English,
        Language::Esperanto,
        Language::Estonian,
        Language::Finnish,
        Language::French,
        Language::German,
        Language::Greek,
        Language::Hindi,
        Language::Hungarian,
        Language::Indonesian,
        Language::Irish,
        Language::Italian,
        Language::Latvian,
        Language::Lithuanian,
        Language::Nepali,
        Language::Norwegian,
        Language::Persian,
        Language::Polish,
        Language::Portuguese,
        Language::Romanian,
        Language::Russian,
        Language::Serbian,
        Language::Slovak,
        Language::Sorani,
        Language::Sotho,
        Language::Spanish,
        Language::Swedish,
        Language::Tamil,
        Language::Telugu,
        Language::Turkish,
        Language::Ukrainian,
    ];
}

/// Which implementation stems a given [`Language`].
#[derive(Clone, Copy)]
enum StemmerAlgorithm {
    /// A Snowball algorithm, via `frostem`.
    Snowball(Algorithm),
    /// A stemmer ported from Apache Lucene, for a language Snowball does not cover.
    Light(LightAlgorithm),
}

/// A stemmer instantiated for one token stream.
enum StemmerImpl {
    Snowball(frostem::Stemmer),
    Light {
        algorithm: LightAlgorithm,
        /// Reused across tokens so stemming does not allocate a character buffer per token.
        scratch: Vec<char>,
    },
}

impl StemmerImpl {
    fn new(algorithm: StemmerAlgorithm) -> Self {
        match algorithm {
            StemmerAlgorithm::Snowball(algorithm) => {
                StemmerImpl::Snowball(frostem::Stemmer::new(algorithm))
            }
            StemmerAlgorithm::Light(algorithm) => StemmerImpl::Light {
                algorithm,
                scratch: Vec::new(),
            },
        }
    }

    fn stem<'a>(&mut self, word: &'a str) -> Cow<'a, str> {
        match self {
            StemmerImpl::Snowball(stemmer) => stemmer.stem(word),
            StemmerImpl::Light { algorithm, scratch } => algorithm.stem(word, scratch),
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

impl<T: Tokenizer> Tokenizer for StemmerFilter<T> {
    type TokenStream<'a> = StemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let stemmer = StemmerImpl::new(self.stemmer_algorithm);
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
        let stemmed_str = self.stemmer.stem(&token.text);
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

#[cfg(test)]
mod tests {
    use tokenizer_api::Token;

    use super::*;
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{
        LowerCaser, RawTokenizer, SimpleTokenizer, TextAnalyzer, TokenizerManager,
    };

    /// `Language::ALL` lists every variant.
    ///
    /// Adding a variant breaks the match below (it stops being exhaustive) and trips the length
    /// assertion, so a new language cannot be added without updating `ALL`.
    #[test]
    fn test_all_languages_are_listed() {
        for language in Language::ALL {
            #[allow(clippy::match_same_arms)]
            match language {
                Language::Arabic | Language::Armenian | Language::Basque => {}
                Language::Bengali | Language::Bulgarian | Language::Catalan => {}
                Language::Croatian | Language::Slovak => {}
                Language::Czech | Language::Danish | Language::Dutch | Language::English => {}
                Language::Esperanto | Language::Estonian | Language::Finnish => {}
                Language::French | Language::German | Language::Greek | Language::Hindi => {}
                Language::Hungarian | Language::Indonesian | Language::Irish => {}
                Language::Italian | Language::Latvian | Language::Lithuanian => {}
                Language::Nepali | Language::Norwegian | Language::Persian => {}
                Language::Polish | Language::Portuguese | Language::Romanian => {}
                Language::Russian | Language::Serbian | Language::Sorani | Language::Sotho => {}
                Language::Spanish | Language::Swedish | Language::Tamil => {}
                Language::Telugu | Language::Turkish | Language::Ukrainian => {}
            }
        }
        assert_eq!(Language::ALL.len(), 41);
    }

    /// Every language can be stemmed.
    ///
    /// This is what fails loudly if a `frostem` Cargo feature is missing or misspelled, rather
    /// than the build failing somewhere less obvious.
    #[test]
    fn test_every_language_stems() {
        for &language in Language::ALL {
            let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(Stemmer::new(language))
                .build();
            let mut tokens = vec![];
            analyzer
                .token_stream("the quick brown foxes")
                .process(&mut |token: &Token| tokens.push(token.text.clone()));
            assert_eq!(tokens.len(), 4, "{language:?} dropped or added tokens");
            assert!(
                tokens.iter().all(|t| !t.is_empty()),
                "{language:?} produced an empty token"
            );
        }
    }

    /// Ukrainian, which arrived with the `frostem` 2.x upgrade.
    #[test]
    fn test_ukrainian_stems() {
        for (word, expected) in [
            ("книга", "книг"),
            ("книги", "книг"),
            ("книгами", "книг"),
            ("великий", "велик"),
            ("великого", "велик"),
            ("місто", "міст"),
            ("міста", "міст"),
            ("містах", "міст"),
        ] {
            let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(Stemmer::new(Language::Ukrainian))
                .build();
            let mut tokens = vec![];
            analyzer
                .token_stream(word)
                .process(&mut |token: &Token| tokens.push(token.text.clone()));
            assert_eq!(tokens, vec![expected.to_string()], "{word}");
        }
    }

    /// Croatian is served by Snowball's Serbian algorithm, which is the Ljubesic-Pandzic
    /// Croatian stemmer with a Cyrillic-to-Latin prelude.
    ///
    /// The last case is the interesting one: the Cyrillic and Latin spellings of the same word
    /// conflate to one stem, which is what makes the shared algorithm correct for Croatian,
    /// Bosnian and both scripts of Serbian.
    #[test]
    fn test_croatian_uses_the_serbian_algorithm() {
        for (word, expected) in [
            ("kuća", "kuć"),
            ("kućama", "kuć"),
            ("veliki", "velik"),
            ("velikih", "velik"),
            ("knjiga", "knjig"),
            ("knjigama", "knjig"),
            // Cyrillic `knjiga`, transliterated by the algorithm's prelude.
            ("књига", "knjig"),
        ] {
            let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(Stemmer::new(Language::Croatian))
                .build();
            let mut tokens = vec![];
            analyzer
                .token_stream(word)
                .process(&mut |token: &Token| tokens.push(token.text.clone()));
            assert_eq!(tokens, vec![expected.to_string()], "{word}");
        }
    }

    /// The five ported stemmers are reachable through the public API and actually stem.
    #[test]
    fn test_ported_stemmers_are_wired_up() {
        for (language, word, expected) in [
            (Language::Bulgarian, "градовете", "град"),
            (Language::Latvian, "tēviem", "tēv"),
            (Language::Bengali, "করেছিলাম", "কর"),
            (Language::Telugu, "ఆమెను", "ఆమె"),
            (Language::Sorani, "پیاوێک", "پیاو"),
            (Language::Slovak, "pruskými", "prusk"),
        ] {
            let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(Stemmer::new(language))
                .build();
            let mut tokens = vec![];
            analyzer
                .token_stream(word)
                .process(&mut |token: &Token| tokens.push(token.text.clone()));
            assert_eq!(tokens, vec![expected.to_string()], "{language:?}");
        }
    }

    #[test]
    fn test_en_stem() {
        let tokenizer_manager = TokenizerManager::default();
        let mut en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            en_tokenizer
                .token_stream("Dogs are the bests!")
                .process(&mut add_token);
        }

        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "dog", 0, 4);
        assert_token(&tokens[1], 1, "are", 5, 8);
        assert_token(&tokens[2], 2, "the", 9, 12);
        assert_token(&tokens[3], 3, "best", 13, 18);
    }

    #[test]
    fn test_non_en_stem() {
        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register(
            "el_stem",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::Greek))
                .build(),
        );
        let mut el_tokenizer = tokenizer_manager.get("el_stem").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            el_tokenizer
                .token_stream("Καλημέρα, χαρούμενε φορολογούμενε!")
                .process(&mut add_token);
        }

        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "καλημερ", 0, 16);
        assert_token(&tokens[1], 1, "χαρουμεν", 18, 36);
        assert_token(&tokens[2], 2, "φορολογουμεν", 37, 63);
    }

    /// Regression for a multibyte Greek suffix that could panic in the
    /// unmaintained `rust-stemmers` 1.2.0 Greek implementation after the
    /// stemmer shortened a word using stale UTF-8 byte offsets.
    #[test]
    fn test_greek_stemmer_handles_multibyte_suffixes() {
        let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(Stemmer::new(Language::Greek))
            .build();
        let mut stream = analyzer.token_stream("αντιθετε");

        assert!(stream.advance());
        assert_eq!(stream.token().text, "ανετ");
    }
}
