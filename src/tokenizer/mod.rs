//! Tokenizer are in charge of processing text for indexing.
//!
//! An tokenizer is a configurable pipeline that starts by a `Tokenizer`,
//! followed by a sequence of [`TokenFilter`s](./trait.TokenFilter.html) to it.
//!
//! The `Tokenizer` is in charge of chopping the text into tokens. There is no
//! trait called `Tokenizer`. Instead `Tokenizer` like [`SimpleTokenizer`](./struct.SimpleTokenizer.html)
//! are just directly implementing the tokenizer trait.
//!
//! - choosing a tokenizer. A tokenizer is in charge of chopping your text into token.
//! - adding so called filter to modify your tokens (e.g. filter out stop words, apply stemming etc.)
//!
//! # Example
//!
//! ```
//! extern crate tantivy;
//! use tantivy::tokenizer::*;
//!
//! // ...
//!
//! # fn main() {
//! let mut tokenizer = SimpleTokenizer
//!        .filter(RemoveLongFilter::limit(40))
//!        .filter(LowerCaser);
//! tokenizer
//!     .token_stream("Hello, happy tax payer")
//!     .process(&mut |token| {
//!         println!("token {:?}", token.text);
//!     });
//! # }
//! ```

mod tokenizer;
mod simple_tokenizer;
mod lower_caser;
mod remove_long;
mod stemmer;
mod tokenizer_manager;
mod japanese_tokenizer;
mod token_stream_chain;
mod raw_tokenizer;


pub use self::tokenizer::{box_tokenizer, Tokenizer, Token, TokenFilterFactory, TokenStream};
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
        let mut en_tokenizer = tokenizer_manager.get("raw").unwrap();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer.token_stream("Hello, happy tax payer!").process(&mut add_token);
        }
        assert_eq!(tokens.len(), 1);
        assert_eq!(&tokens[0], "Hello, happy tax payer!");
    }


    #[test]
    fn test_en_tokenizer() {
        let tokenizer_manager = TokenizerManager::default();
        assert!(tokenizer_manager.get("en_doesnotexist").is_none());
        let mut en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer.token_stream("Hello, happy tax payer!").process(&mut add_token);
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
        let mut en_tokenizer = tokenizer_manager.get("ja").unwrap();
        
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.text.clone()); };
            en_tokenizer.token_stream("野菜食べないとやばい!").process(&mut add_token);
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
        let mut en_tokenizer = tokenizer_manager.get("en_stem").unwrap();
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
