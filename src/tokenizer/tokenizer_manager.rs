use crate::tokenizer::stemmer::Language;
use crate::tokenizer::tokenizer::{TextAnalyzer, TextAnalyzerT, Tokenizer};
use crate::tokenizer::LowerCaser;
use crate::tokenizer::RawTokenizer;
use crate::tokenizer::RemoveLongFilter;
use crate::tokenizer::SimpleTokenizer;
use crate::tokenizer::Stemmer;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
#[derive(Clone)]
pub struct TokenizerManager<'a> {
    tokenizers: Arc<RwLock<HashMap<String, Box<dyn TextAnalyzerT<'a>>>>>,
}

impl<'a> TokenizerManager<'a> {
    /// Registers a new tokenizer associated with a given name.
    pub fn register<T>(&self, tokenizer_name: &str, tokenizer: T)
    where
        T: TextAnalyzerT<'a>,
    {
        self.tokenizers
            .write()
            .expect("Acquiring the lock should never fail")
            .insert(tokenizer_name.to_string(), Box::new(tokenizer));
    }

    /// Accessing a tokenizer given its name.
    pub fn get(&self, tokenizer_name: &str) -> Option<Box<dyn TextAnalyzerT<'a>>> {
        self.tokenizers
            .read()
            .expect("Acquiring the lock should never fail")
            .get(tokenizer_name)
            .cloned()
    }
}

impl<'a> Default for TokenizerManager<'a> {
    /// Creates an `TokenizerManager` prepopulated with
    /// the default pre-configured tokenizers of `tantivy`.
    /// - simple
    /// - en_stem
    /// - ja
    fn default() -> Self {
        let manager = TokenizerManager {
            tokenizers: Arc::new(RwLock::new(HashMap::new())),
        };
        manager.register("raw", TextAnalyzer::new(RawTokenizer));
        manager.register(
            "default",
            TextAnalyzer::new(SimpleTokenizer)
                .filter(RemoveLongFilter::new(40))
                .filter(LowerCaser::new()),
        );
        manager.register(
            "en_stem",
            TextAnalyzer::new(SimpleTokenizer)
                .filter(RemoveLongFilter::new(40))
                .filter(LowerCaser::new())
                .filter(Stemmer::new(Language::English)),
        );
        manager
    }
}
