use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::tokenizer::stemmer::Language;
use crate::tokenizer::tokenizer::TextAnalyzer;
use crate::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer, WhitespaceTokenizer,
};

/// The tokenizer manager serves as a store for
/// all of the pre-configured tokenizer pipelines.
///
/// By default, it is populated with the following managers.
///
///  * `raw` : does not process nor tokenize the text.
///  * `default` : Chops the text on according to whitespace and
///  punctuation, removes tokens that are too long, and lowercases
///  tokens
///  * `en_stem` : Like `default`, but also applies stemming on the
///  resulting tokens. Stemming can improve the recall of your
///  search engine.
/// * `whitespace` : Splits the text on whitespaces.
#[derive(Clone)]
pub struct TokenizerManager {
    tokenizers: Arc<RwLock<HashMap<String, TextAnalyzer>>>,
}

impl TokenizerManager {
    /// Creates an empty tokenizer manager.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            tokenizers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a new tokenizer associated with a given name.
    pub fn register<T>(&self, tokenizer_name: &str, tokenizer: T)
    where TextAnalyzer: From<T> {
        let boxed_tokenizer: TextAnalyzer = TextAnalyzer::from(tokenizer);
        self.tokenizers
            .write()
            .expect("Acquiring the lock should never fail")
            .insert(tokenizer_name.to_string(), boxed_tokenizer);
    }

    /// Accessing a tokenizer given its name.
    pub fn get(&self, tokenizer_name: &str) -> Option<TextAnalyzer> {
        self.tokenizers
            .read()
            .expect("Acquiring the lock should never fail")
            .get(tokenizer_name)
            .cloned()
    }

    /// Creates an `TokenizerManager` prepopulated with
    /// the default pre-configured tokenizers of `tantivy`.
    pub fn default_for_indexing() -> TokenizerManager {
        let manager = TokenizerManager::new();
        manager.register("raw", RawTokenizer::default());
        manager.register(
            "default",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .build(),
        );
        manager.register(
            "en_stem",
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::English))
                .build(),
        );
        manager.register("whitespace", WhitespaceTokenizer::default());
        manager
    }

    /// Creates an `TokenizerManager` prepopulated with
    /// the default pre-configured tokenizers of `tantivy`
    /// for fast fields.
    ///
    /// Fast fields usually do not really tokenize the text.
    /// It is however very useful to filter / normalize the text.
    pub fn default_for_fast_fields() -> TokenizerManager {
        let manager = TokenizerManager::new();
        let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(RemoveLongFilter::limit(255))
            .build();
        let lower_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(RemoveLongFilter::limit(255))
            .filter(LowerCaser)
            .build();
        manager.register(
            crate::schema::DEFAULT_FAST_FIELD_TOKENIZER,
            lower_tokenizer.clone(),
        );
        manager.register("raw", raw_tokenizer);
        manager.register("lower", lower_tokenizer);
        manager
    }
}
