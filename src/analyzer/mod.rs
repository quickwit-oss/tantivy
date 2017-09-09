mod analyzer;
mod simple_tokenizer;
mod lower_caser;
mod remove_long;
mod stemmer;
mod analyzer_manager;
mod japanese_tokenizer;
mod token_stream_chain;


pub use self::analyzer::{box_analyzer, Analyzer, Token, TokenFilterFactory, TokenStream};
pub use self::analyzer::BoxedAnalyzer;
pub use self::analyzer_manager::AnalyzerManager;
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::token_stream_chain::TokenStreamChain;
pub use self::japanese_tokenizer::JapaneseTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::lower_caser::LowerCaser;
pub use self::stemmer::Stemmer;

#[cfg(test)]
mod test {
    use super::Token;
    use super::AnalyzerManager;

    #[test]
    fn test_en_analyzer() {
        let analyzer_manager = AnalyzerManager::default();
        assert!(analyzer_manager.get("en_doesnotexist").is_none());
        let mut en_analyzer = analyzer_manager.get("en_stem").unwrap();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
            en_analyzer.token_stream("hello, happy tax payer!").process(&mut add_token);
        }
        assert_eq!(tokens.len(), 4);
        assert_eq!(&tokens[0], "hello");
        assert_eq!(&tokens[1], "happi");
        assert_eq!(&tokens[2], "tax");
        assert_eq!(&tokens[3], "payer");
    }

    #[test]
    fn test_jp_analyzer() {
        let analyzer_manager = AnalyzerManager::default();
        let mut en_analyzer = analyzer_manager.get("ja").unwrap();
        
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
            en_analyzer.token_stream("野菜食べないとやばい!").process(&mut add_token);
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
        let analyzer_manager = AnalyzerManager::default();
        let mut en_analyzer = analyzer_manager.get("en_stem").unwrap();
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
                en_analyzer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
                en_analyzer.token_stream(" ").process(&mut add_token);
            }
            assert!(tokens.is_empty());
        }
    }

}
