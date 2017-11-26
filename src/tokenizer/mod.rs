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
//! register it with a name in your index's [TokenizerManager](./struct.TokenizerManager.html).
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
mod tokenizer;
mod simple_tokenizer;
mod lower_caser;
mod remove_long;
mod stemmer;
mod tokenizer_manager;
mod japanese_tokenizer;
mod token_stream_chain;
mod raw_tokenizer;


pub use self::tokenizer::{box_tokenizer, Tokenizer, Token, TokenFilter, TokenStream};
pub use self::tokenizer::BoxedTokenizer;
pub use self::tokenizer_manager::TokenizerManager;
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::raw_tokenizer::RawTokenizer;
pub use self::token_stream_chain::TokenStreamChain;
pub use self::japanese_tokenizer::JapaneseTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::lower_caser::LowerCaser;
pub use self::stemmer::Stemmer;

#[cfg(test)]
mod test {
    use super::Token;
    use super::TokenizerManager;


    #[test]
    fn test_raw_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("raw").unwrap();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer
                .token_stream("Hello, happy tax payer!")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 1);
        assert_eq!(&tokens[0], "Hello, happy tax payer!");
    }


    #[test]
    fn test_en_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        assert!(tokenizer_manager.get("en_doesnotexist").is_none());
        let en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer
                .token_stream("Hello, happy tax payer!")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 4);
        assert_eq!(&tokens[0], "hello");
        assert_eq!(&tokens[1], "happi");
        assert_eq!(&tokens[2], "tax");
        assert_eq!(&tokens[3], "payer");
    }

    #[test]
    fn test_jp_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("ja").unwrap();

        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer
                .token_stream("野菜食べないとやばい!")
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 5);
        assert_eq!(&tokens[0], "野菜");
        assert_eq!(&tokens[1], "食べ");
        assert_eq!(&tokens[2], "ない");
        assert_eq!(&tokens[3], "と");
        assert_eq!(&tokens[4], "やばい");
    }

    #[test]
    fn test_tokenizer_empty() {
        let tokenizer_manager = TokenizerManager::default();
        let en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
                en_tokenizer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
                en_tokenizer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
    }

}
