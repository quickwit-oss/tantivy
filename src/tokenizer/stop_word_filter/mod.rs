//! # Example
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
//!   .filter(StopWordFilter::remove(vec!["the".to_string(), "is".to_string()]))
//!   .build();
//!
//! let mut stream = tokenizer.token_stream("the fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! ```
//!
//! Tantivy also ships a stop word list for every [`Language`](super::Language), reachable with
//! [`StopWordFilter::new`]:
//!
//! ```rust
//! # #[cfg(feature = "stemmer")] {
//! use tantivy::tokenizer::*;
//!
//! let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
//!   .filter(LowerCaser)
//!   .filter(StopWordFilter::new(Language::English).unwrap())
//!   .build();
//!
//! let mut stream = tokenizer.token_stream("The fox is crafty");
//! assert_eq!(stream.next().unwrap().text, "fox");
//! assert_eq!(stream.next().unwrap().text, "crafty");
//! assert!(stream.next().is_none());
//! # }
//! ```
// The built-in lists are only reachable through `new(Language)`, and `Language` comes from the
// `stemmer` feature, so without it they would be dead code. `StopWordFilter::remove` still works
// with a caller-supplied list regardless of either feature.
#[cfg(all(feature = "stopwords", feature = "stemmer"))]
#[rustfmt::skip]
mod stopwords;

use std::sync::Arc;

use rustc_hash::FxHashSet;

#[cfg(all(feature = "stopwords", feature = "stemmer"))]
use super::Language;
use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// `TokenFilter` that removes stop words from a token stream
#[derive(Clone)]
pub struct StopWordFilter {
    words: Arc<FxHashSet<String>>,
}

impl StopWordFilter {
    /// Creates a new [`StopWordFilter`] for the given [`Language`]
    ///
    /// Every [`Language`] has a stop word list, so this always returns `Some`. The `Option` is
    /// kept for backwards compatibility.
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    pub fn new(language: Language) -> Option<Self> {
        // This match is deliberately exhaustive, with no catch-all arm: adding a `Language`
        // without also adding its stop word list must be a compile error rather than a silently
        // empty filter.
        let words = match language {
            Language::Arabic => stopwords::ARABIC,
            Language::Armenian => stopwords::ARMENIAN,
            Language::Basque => stopwords::BASQUE,
            Language::Bengali => stopwords::BENGALI,
            Language::Bulgarian => stopwords::BULGARIAN,
            Language::Catalan => stopwords::CATALAN,
            Language::Croatian => stopwords::CROATIAN,
            Language::Czech => stopwords::CZECH,
            Language::Danish => stopwords::DANISH,
            Language::Dutch => stopwords::DUTCH,
            Language::English => {
                // This is the same list of words used by the Apache-licensed Lucene project,
                // c.f. https://github.com/apache/lucene/blob/d5d6dc079395c47cd6d12dcce3bcfdd2c7d9dc63/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/EnglishAnalyzer.java#L46
                &[
                    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
                    "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
                    "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
                ]
            }
            Language::Esperanto => stopwords::ESPERANTO,
            Language::Estonian => stopwords::ESTONIAN,
            Language::Finnish => stopwords::FINNISH,
            Language::French => stopwords::FRENCH,
            Language::German => stopwords::GERMAN,
            Language::Greek => stopwords::GREEK,
            Language::Hindi => stopwords::HINDI,
            Language::Hungarian => stopwords::HUNGARIAN,
            Language::Indonesian => stopwords::INDONESIAN,
            Language::Irish => stopwords::IRISH,
            Language::Italian => stopwords::ITALIAN,
            Language::Latvian => stopwords::LATVIAN,
            Language::Lithuanian => stopwords::LITHUANIAN,
            Language::Nepali => stopwords::NEPALI,
            Language::Norwegian => stopwords::NORWEGIAN,
            Language::Persian => stopwords::PERSIAN,
            Language::Polish => stopwords::POLISH,
            Language::Portuguese => stopwords::PORTUGUESE,
            Language::Romanian => stopwords::ROMANIAN,
            Language::Russian => stopwords::RUSSIAN,
            Language::Serbian => stopwords::SERBIAN,
            Language::Slovak => stopwords::SLOVAK,
            Language::Sorani => stopwords::SORANI,
            Language::Sotho => stopwords::SOTHO,
            Language::Spanish => stopwords::SPANISH,
            Language::Swedish => stopwords::SWEDISH,
            Language::Tamil => stopwords::TAMIL,
            Language::Telugu => stopwords::TELUGU,
            Language::Turkish => stopwords::TURKISH,
            Language::Ukrainian => stopwords::UKRAINIAN,
        };

        Some(Self::remove(words.iter().map(|&word| word.to_owned())))
    }

    /// The words this filter removes.
    #[cfg(test)]
    pub(crate) fn words(&self) -> &FxHashSet<String> {
        &self.words
    }

    /// Creates a `StopWordFilter` given a list of words to remove
    pub fn remove<W: IntoIterator<Item = String>>(words: W) -> StopWordFilter {
        StopWordFilter {
            words: Arc::new(words.into_iter().collect()),
        }
    }
}

impl TokenFilter for StopWordFilter {
    type Tokenizer<T: Tokenizer> = StopWordFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> StopWordFilterWrapper<T> {
        StopWordFilterWrapper {
            words: self.words,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct StopWordFilterWrapper<T> {
    words: Arc<FxHashSet<String>>,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for StopWordFilterWrapper<T> {
    type TokenStream<'a> = StopWordFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StopWordFilterStream {
            words: self.words.clone(),
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct StopWordFilterStream<T> {
    words: Arc<FxHashSet<String>>,
    tail: T,
}

impl<T> StopWordFilterStream<T> {
    fn predicate(&self, token: &Token) -> bool {
        !self.words.contains(&token.text)
    }
}

impl<T: TokenStream> TokenStream for StopWordFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.predicate(self.tail.token()) {
                return true;
            }
        }
        false
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
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{SimpleTokenizer, StopWordFilter, TextAnalyzer, Token};

    /// Word counts of the thirteen lists that predate the language expansion, which must not
    /// change. Snowball serves its lists from a path with no revision handle and has already
    /// drifted since they were vendored (its Finnish list has gained a word), so these counts are
    /// the guard that a regeneration carried the original data forward unaltered.
    ///
    /// These are set sizes, which is what the filter actually holds. Two of the vendored lists
    /// contain duplicate entries -- Finnish five and Hungarian one -- so their counts here are
    /// lower than the number of lines in `stopwords.rs`. The duplicates are kept in the data
    /// deliberately: the words go into a set, so dropping them would change nothing except the
    /// data's fidelity to what was vendored.
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    const FROZEN_LIST_SIZES: &[(super::Language, usize)] = &[
        (super::Language::Danish, 94),
        (super::Language::Dutch, 101),
        (super::Language::English, 33),
        (super::Language::Finnish, 229),
        (super::Language::French, 154),
        (super::Language::German, 231),
        (super::Language::Hungarian, 198),
        (super::Language::Italian, 279),
        (super::Language::Norwegian, 172),
        (super::Language::Portuguese, 203),
        (super::Language::Russian, 159),
        (super::Language::Spanish, 308),
        (super::Language::Swedish, 114),
    ];

    /// Every language has a non-empty built-in list.
    ///
    /// The exhaustive match in [`StopWordFilter::new`] already makes a missing list a compile
    /// error; this checks the lists are actually populated.
    #[test]
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    fn test_every_language_has_stop_words() {
        for language in super::Language::ALL {
            let filter = StopWordFilter::new(*language)
                .unwrap_or_else(|| panic!("{language:?} has no stop word list"));
            assert!(
                !filter.words().is_empty(),
                "{language:?} has an empty stop word list"
            );
        }
    }

    #[test]
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    fn test_frozen_lists_are_unchanged() {
        for &(language, expected) in FROZEN_LIST_SIZES {
            let filter = StopWordFilter::new(language).unwrap();
            assert_eq!(
                filter.words().len(),
                expected,
                "{language:?}'s stop word list changed size; it is frozen and must not be \
                 regenerated from upstream"
            );
        }
    }

    /// Hygiene checks for the lists added by the language expansion.
    ///
    /// The script-specific guards are the ones that matter: stopwords-iso's Lithuanian list is
    /// about 27% Windows-1257/Latin-1 mojibake, which is why Lithuanian is sourced from spaCy
    /// instead, and this is what would catch a regression back to corrupt data.
    #[test]
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    fn test_added_lists_are_well_formed() {
        const LITHUANIAN_ALPHABET: &str = "aąbcčdeęėfghiįyjklmnoprsštuųūvzž";
        let frozen: Vec<super::Language> = FROZEN_LIST_SIZES.iter().map(|&(l, _)| l).collect();

        for language in super::Language::ALL {
            if frozen.contains(language) {
                continue;
            }
            let filter = StopWordFilter::new(*language).unwrap();
            for word in filter.words() {
                assert!(!word.is_empty(), "{language:?} has an empty entry");
                assert_eq!(word.trim(), word, "{language:?}: {word:?} is not trimmed");
                assert!(
                    !word.chars().any(char::is_whitespace),
                    "{language:?}: {word:?} contains whitespace, so it can never match a token"
                );
                assert!(
                    !word.chars().any(char::is_uppercase),
                    "{language:?}: {word:?} is not lowercased"
                );
                let letters = || word.chars().filter(|c| c.is_alphabetic());
                match language {
                    super::Language::Lithuanian => assert!(
                        letters().all(|c| LITHUANIAN_ALPHABET.contains(c)),
                        "{word:?} is not Lithuanian; the list may have reverted to the corrupt \
                         stopwords-iso data"
                    ),
                    super::Language::Bulgarian | super::Language::Serbian => assert!(
                        letters().all(|c| ('\u{400}'..='\u{4ff}').contains(&c)),
                        "{language:?}: {word:?} is not Cyrillic"
                    ),
                    super::Language::Sorani => assert!(
                        letters().all(|c| ('\u{600}'..='\u{6ff}').contains(&c)),
                        "{word:?} is not Arabic script; stopwords-iso's `ku` list is Sorani"
                    ),
                    _ => {}
                }
            }
        }
    }

    /// End-to-end coverage of a built-in list, for one of the languages that returned `None`
    /// before the expansion.
    #[test]
    #[cfg(all(feature = "stopwords", feature = "stemmer"))]
    fn test_built_in_list_filters_turkish() {
        use crate::tokenizer::LowerCaser;

        let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(StopWordFilter::new(super::Language::Turkish).unwrap())
            .build();
        let mut tokens = vec![];
        analyzer
            .token_stream("Ben ve sen kitap okuyoruz")
            .process(&mut |token: &Token| tokens.push(token.text.clone()));
        // `ve` ("and") is in NLTK's Turkish list; the pronouns and content words are not.
        assert_eq!(
            tokens,
            vec![
                "ben".to_string(),
                "sen".into(),
                "kitap".into(),
                "okuyoruz".into()
            ]
        );
    }

    #[test]
    fn test_stop_word() {
        let tokens = token_stream_helper("i am a cat. as yet i have no name.");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 3, "cat", 7, 10);
        assert_token(&tokens[1], 5, "yet", 15, 18);
        assert_token(&tokens[2], 7, "have", 21, 25);
        assert_token(&tokens[3], 8, "no", 26, 28);
        assert_token(&tokens[4], 9, "name", 29, 33);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let stops = vec![
            "a".to_string(),
            "as".to_string(),
            "am".to_string(),
            "i".to_string(),
        ];
        let mut a = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(StopWordFilter::remove(stops))
            .build();
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
