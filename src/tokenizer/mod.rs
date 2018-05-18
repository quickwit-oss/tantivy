//! Tokenizer are in charge of chopping text into a stream of tokens
//! ready for indexing.
//!
//! You must define in your schema which tokenizer should be used for
//! each of your fields :
//!
//! ```
//! extern crate tantivy;
//! use tantivy::schema::*;
//!
//! # fn main() {
//! let mut schema_builder = SchemaBuilder::new();
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
//! # }
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
//! # extern crate tantivy;
//!
//! use tantivy::tokenizer::*;
//!
//! # fn main() {
//! let en_stem = SimpleTokenizer
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser)
//!     .filter(Stemmer::new());
//! # }
//! ```
//!
//! Once your tokenizer is defined, you need to
//! register it with a name in your index's [`TokenizerManager`](./struct.TokenizerManager.html).
//!
//! ```
//! # extern crate tantivy;
//! # use tantivy::schema::SchemaBuilder;
//! # use tantivy::tokenizer::*;
//! # use tantivy::Index;
//! # fn main() {
//! # let custom_en_tokenizer = SimpleTokenizer;
//! # let schema = SchemaBuilder::new().build();
//! let index = Index::create_in_ram(schema);
//! index.tokenizers()
//!      .register("custom_en", custom_en_tokenizer);
//! # }
//! ```
//!
//! If you built your schema programmatically, a complete example
//! could like this for instance.
//!
//! # Example
//!
//! ```
//! extern crate tantivy;
//! use tantivy::schema::{SchemaBuilder, IndexRecordOption, TextOptions, TextFieldIndexing};
//! use tantivy::tokenizer::*;
//! use tantivy::Index;
//!
//! # fn main() {
//! let mut schema_builder = SchemaBuilder::new();
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
//! let custom_en_tokenizer = SimpleTokenizer
//!     .filter(RemoveLongFilter::limit(40))
//!     .filter(LowerCaser);
//! index
//!     .tokenizers()
//!     .register("custom_en", custom_en_tokenizer);
//! // ...
//! # }
//! ```
//!
mod alphanum_only;
mod facet_tokenizer;
mod japanese_tokenizer;
mod lower_caser;
mod ngram_tokenizer;
mod raw_tokenizer;
mod remove_long;
mod simple_tokenizer;
mod stemmer;
mod stop_word_filter;
mod token_stream_chain;
mod tokenizer;
mod tokenizer_manager;

pub use self::alphanum_only::AlphaNumOnlyFilter;
pub use self::facet_tokenizer::FacetTokenizer;
pub use self::japanese_tokenizer::JapaneseTokenizer;
pub use self::lower_caser::LowerCaser;
pub use self::ngram_tokenizer::NgramTokenizer;
pub use self::raw_tokenizer::RawTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::stemmer::Stemmer;
pub use self::stop_word_filter::StopWordFilter;
pub(crate) use self::token_stream_chain::TokenStreamChain;
pub use self::tokenizer::BoxedTokenizer;
pub use self::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};
pub use self::tokenizer_manager::TokenizerManager;

/// This is a function that can be used in tests and doc tests
/// to assert a token's correctness.
/// TODO: can this be wrapped in #[cfg(test)] so as not to be in the
/// public api?
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

#[cfg(test)]
pub mod test {
    use super::assert_token;
    use super::Token;
    use super::TokenizerManager;

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
    fn test_jp_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("ja").unwrap();

        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            en_tokenizer
                .token_stream("野菜食べないとやばい!")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 0, "野菜", 0, 6);
        assert_token(&tokens[1], 1, "食べ", 6, 12);
        assert_token(&tokens[2], 2, "ない", 12, 18);
        assert_token(&tokens[3], 3, "と", 18, 21);
        assert_token(&tokens[4], 4, "やばい", 21, 30);
    }

    #[test]
    fn test_ngram_tokenizer() {
        use super::{LowerCaser, NgramTokenizer};
        use tokenizer::tokenizer::TokenStream;
        use tokenizer::tokenizer::Tokenizer;

        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register("ngram12", NgramTokenizer::new(1, 2, false));
        tokenizer_manager.register(
            "ngram3",
            NgramTokenizer::new(3, 3, false).filter(LowerCaser),
        );
        tokenizer_manager.register(
            "edgegram5",
            NgramTokenizer::new(2, 5, true).filter(LowerCaser),
        );

        let tokenizer = NgramTokenizer::new(1, 2, false);
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            tokenizer.token_stream("hello").process(&mut add_token);
        }
        assert_eq!(tokens.len(), 9);
        assert_token(&tokens[0], 0, "h", 0, 1);
        assert_token(&tokens[1], 0, "he", 0, 2);
        assert_token(&tokens[2], 1, "e", 1, 2);
        assert_token(&tokens[3], 1, "el", 1, 3);
        assert_token(&tokens[4], 2, "l", 2, 3);
        assert_token(&tokens[5], 2, "ll", 2, 4);
        assert_token(&tokens[6], 3, "l", 3, 4);
        assert_token(&tokens[7], 3, "lo", 3, 5);
        assert_token(&tokens[8], 4, "o", 4, 5);

        let tokenizer = tokenizer_manager.get("ngram3").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            tokenizer.token_stream("Hello").process(&mut add_token);
        }
        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "hel", 0, 3);
        assert_token(&tokens[1], 1, "ell", 1, 4);
        assert_token(&tokens[2], 2, "llo", 2, 5);

        let tokenizer = tokenizer_manager.get("edgegram5").unwrap();
        let mut tokens: Vec<Token> = vec![];
        {
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            tokenizer
                .token_stream("Frankenstein")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "fr", 0, 2);
        assert_token(&tokens[1], 0, "fra", 0, 3);
        assert_token(&tokens[2], 0, "fran", 0, 4);
        assert_token(&tokens[3], 0, "frank", 0, 5);
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
