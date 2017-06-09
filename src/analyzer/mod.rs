extern crate regex;

mod analyzer;
mod simple_tokenizer;
mod lower_caser;
mod remove_long;
mod remove_nonalphanum;
mod stemmer;
mod jp_tokenizer;

pub use self::analyzer::{boxed_pipeline, TextPipeline, Analyzer, Token, TokenFilterFactory,
                         TokenStream};
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::jp_tokenizer::JpTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::lower_caser::LowerCaser;
pub use self::stemmer::Stemmer;
pub use self::remove_nonalphanum::RemoveNonAlphaFilter;


pub fn en_pipeline<'a>() -> Box<TextPipeline> {
    boxed_pipeline(SimpleTokenizer
                       .filter(RemoveLongFilter::limit(20))
                       .filter(LowerCaser)
                       .filter(Stemmer::new()))
}

pub fn jp_pipeline<'a>() -> Box<TextPipeline> {
    boxed_pipeline(JpTokenizer
                       .filter(RemoveLongFilter::limit(20))
                       .filter(RemoveNonAlphaFilter))
}

#[cfg(test)]
mod test {
    use super::{en_pipeline, jp_pipeline, Token};

    #[test]
    fn test_en_analyzer() {
        let mut pipeline = en_pipeline();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
            pipeline.analyze("hello, happy tax payer!", &mut add_token);
        }
        assert_eq!(tokens.len(), 4);
        assert_eq!(&tokens[0], "hello");
        assert_eq!(&tokens[1], "happi");
        assert_eq!(&tokens[2], "tax");
        assert_eq!(&tokens[3], "payer");
    }


    #[test]
    fn test_jp_analyzer() {
        let mut pipeline = jp_pipeline();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
            pipeline.analyze("野菜食べないとやばい!", &mut add_token);
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
        let mut pipeline = en_pipeline();
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
                pipeline.analyze(" ", &mut add_token);
            }
            assert!(tokens.is_empty());
        }
        {
            let mut tokens: Vec<String> = vec![];
            {
                let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
                pipeline.analyze(" ", &mut add_token);
            }
            assert!(tokens.is_empty());
        }
    }


    #[test]
    fn test_tokenizer_cjkchars() {
        let mut pipeline = en_pipeline();
        let mut tokens: Vec<String> = vec![];
        {
            let mut add_token = |token: &Token| { tokens.push(token.term.clone()); };
            pipeline.analyze("hello,中国人民", &mut add_token);
        }
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens, vec!["hello", "中国人民"]);
    }
}
