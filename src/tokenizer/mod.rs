//! Tokenizer are in charge of chopping text into a stream of tokens
//! ready for indexing.
//!
//! You must define in your schema which tokenizer should be used for
//! each of your fields :
//!
//! ```rust
//! use tantivy::schema::*;
//!
//! let mut schema_builder = Schema::builder();
//!
//! let text_options = TextOptions::default()
//!     .set_indexing_options(
//!         TextFieldIndexing::default()
//!             .set_tokenizer("en_stem")
//!             .set_index_option(IndexRecordOption::Basic)
//!     )
//!     .set_stored();
//!
//! let id_options = TextOptions::default()
//!     .set_indexing_options(
//!         TextFieldIndexing::default()
//!             .set_tokenizer("raw_ids")
//!             .set_index_option(IndexRecordOption::WithFreqsAndPositions)
//!     )
//!     .set_stored();
//!
//! schema_builder.add_text_field("title", text_options.clone());
//! schema_builder.add_text_field("text", text_options);
//! schema_builder.add_text_field("uuid", id_options);
//!
//! let schema = schema_builder.build();
//! ```
//!
//! By default, `tantivy` offers the following tokenizers:
//!
//! ## `default`
//!
//! `default` is the tokenizer that will be used if you do not
//! assign a specific tokenizer to your text field.
//! It will chop your text on punctuation and whitespaces,
//! removes tokens that are longer than 40 chars, and lowercase your text.
//!
//! ## `raw`
//! Does not actual tokenizer your text. It keeps it entirely unprocessed.
//! It can be useful to index uuids, or urls for instance.
//!
//! ## `en_stem`
//!
//! In addition to what `default` does, the `en_stem` tokenizer also
//! apply stemming to your tokens. Stemming consists in trimming words to
//! remove their inflection. This tokenizer is slower than the default one,
//! but is recommended to improve recall.
//!
//!
//! # Custom tokenizers
//!
//! You can write your own tokenizer by implementing the [`Tokenizer`](./trait.Tokenizer.html)
//! or you can extend an existing [`Tokenizer`](./trait.Tokenizer.html) by chaining it several
//! [`TokenFilter`s](./trait.TokenFilter.html).
//!
//! For instance, the `en_stem` is defined as follows.
//!
//! ```rust
//! use tantivy::tokenizer::*;
//!
//! let en_stem = TextAnalyzer::from(SimpleTokenizer)
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser)
//!     .filter(Stemmer::new(Language::English));
//! ```
//!
//! Once your tokenizer is defined, you need to
//! register it with a name in your index's [`TokenizerManager`](./struct.TokenizerManager.html).
//!
//! ```rust
//! # use tantivy::schema::Schema;
//! # use tantivy::tokenizer::*;
//! # use tantivy::Index;
//! #
//! let custom_en_tokenizer = SimpleTokenizer;
//! # let schema = Schema::builder().build();
//! let index = Index::create_in_ram(schema);
//! index.tokenizers()
//!      .register("custom_en", custom_en_tokenizer);
//! ```
//!
//! If you built your schema programmatically, a complete example
//! could like this for instance.
//!
//! Note that tokens with a len greater or equal to [`MAX_TOKEN_LEN`](./constant.MAX_TOKEN_LEN.html).
//!
//! # Example
//!
//! ```rust
//! use tantivy::schema::{Schema, IndexRecordOption, TextOptions, TextFieldIndexing};
//! use tantivy::tokenizer::*;
//! use tantivy::Index;
//!
//! let mut schema_builder = Schema::builder();
//! let text_field_indexing = TextFieldIndexing::default()
//!     .set_tokenizer("custom_en")
//!     .set_index_option(IndexRecordOption::WithFreqsAndPositions);
//! let text_options = TextOptions::default()
//!     .set_indexing_options(text_field_indexing)
//!     .set_stored();
//! schema_builder.add_text_field("title", text_options);
//! let schema = schema_builder.build();
//! let index = Index::create_in_ram(schema);
//!
//! // We need to register our tokenizer :
//! let custom_en_tokenizer = TextAnalyzer::from(SimpleTokenizer)
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser);
//! index
//!     .tokenizers()
//!     .register("custom_en", custom_en_tokenizer);
//! ```
//!
mod alphanum_only;
mod ascii_folding_filter;
mod facet_tokenizer;
mod lower_caser;
mod ngram_tokenizer;
mod raw_tokenizer;
mod remove_long;
mod simple_tokenizer;
mod stemmer;
mod stop_word_filter;
mod token_stream_chain;
mod tokenized_string;
mod tokenizer;
mod tokenizer_manager;

pub use self::alphanum_only::AlphaNumOnlyFilter;
pub use self::ascii_folding_filter::AsciiFoldingFilter;
pub use self::facet_tokenizer::FacetTokenizer;
pub use self::lower_caser::LowerCaser;
pub use self::ngram_tokenizer::NgramTokenizer;
pub use self::raw_tokenizer::RawTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::stemmer::{Language, Stemmer};
pub use self::stop_word_filter::StopWordFilter;
pub(crate) use self::token_stream_chain::TokenStreamChain;

pub use self::tokenized_string::{PreTokenizedStream, PreTokenizedString};
pub use self::tokenizer::{
    BoxTokenFilter, BoxTokenStream, TextAnalyzer, Token, TokenFilter, TokenStream, Tokenizer,
};

pub use self::tokenizer_manager::TokenizerManager;

/// Maximum authorized len (in bytes) for a token.
///
/// Tokenizer are in charge of not emitting tokens larger than this value.
/// Currently, if a faulty tokenizer implementation emits tokens with a length larger than
/// `2^16 - 1 - 4`, the token will simply be ignored downstream.
pub const MAX_TOKEN_LEN: usize = u16::max_value() as usize - 4;

#[cfg(test)]
pub mod tests {
    use super::{
        Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, Token, TokenizerManager,
    };
    use crate::tokenizer::TextAnalyzer;

    /// This is a function that can be used in tests and doc tests
    /// to assert a token's correctness.
    pub fn assert_token(token: &Token, position: usize, text: &str, from: usize, to: usize) {
        assert_eq!(
            token.position, position,
            "expected position {} but {:?}",
            position, token
        );
        assert_eq!(token.text, text, "expected text {} but {:?}", text, token);
        assert_eq!(
            token.offset_from, from,
            "expected offset_from {} but {:?}",
            from, token
        );
        assert_eq!(
            token.offset_to, to,
            "expected offset_to {} but {:?}",
            to, token
        );
    }

    #[test]
    fn test_raw_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("raw").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            en_tokenizer
                .token_stream("Hello, happy tax payer!")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 1);
        assert_token(&tokens[0], 0, "Hello, happy tax payer!", 0, 23);
    }

    #[test]
    fn test_en_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        assert!(tokenizer_manager.get("en_doesnotexist").is_none());
        let en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            en_tokenizer
                .token_stream("Hello, happy tax payer!")
                .process(&mut add_token);
        }

        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "hello", 0, 5);
        assert_token(&tokens[1], 1, "happi", 7, 12);
        assert_token(&tokens[2], 2, "tax", 13, 16);
        assert_token(&tokens[3], 3, "payer", 17, 22);
    }

    #[test]
    fn test_non_en_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register(
            "el_stem",
            TextAnalyzer::from(SimpleTokenizer)
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::Greek)),
        );
        let en_tokenizer = tokenizer_manager.get("el_stem").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            en_tokenizer
                .token_stream("Καλημέρα, χαρούμενε φορολογούμενε!")
                .process(&mut add_token);
        }

        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "καλημερ", 0, 16);
        assert_token(&tokens[1], 1, "χαρουμεν", 18, 36);
        assert_token(&tokens[2], 2, "φορολογουμεν", 37, 63);
    }

    #[test]
    fn test_tokenizer_empty() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        {
            let mut tokens: Vec<Token> = vec![];
            {
                let mut add_token = |token: &Token| {
                    tokens.push(token.clone());
                };
                en_tokenizer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
        {
            let mut tokens: Vec<Token> = vec![];
            {
                let mut add_token = |token: &Token| {
                    tokens.push(token.clone());
                };
                en_tokenizer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
    }
}
