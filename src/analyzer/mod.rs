extern crate regex;

mod analyzer;
mod simple_tokenizer;
mod lower_caser;
mod remove_long;
mod stemmer;

pub use self::analyzer::{Analyzer, Token, TokenFilterFactory, TokenStream};
pub use self::simple_tokenizer::SimpleTokenizer;
pub use self::remove_long::RemoveLongFilter;
pub use self::lower_caser::LowerCaser;
pub use self::stemmer::Stemmer;



pub fn en_analyzer<'a>() -> impl Analyzer<'a> {
    SimpleTokenizer
        .filter(RemoveLongFilter::limit(20))
        .filter(LowerCaser)
}

#[cfg(test)]
mod test {
    use super::{Analyzer, TokenStream, en_analyzer};

    #[test]
    fn test_tokenizer() {
        let mut analyzer = en_analyzer();
        let mut terms = analyzer.analyze("hello, happy tax payer!");
        assert_eq!(terms.next().unwrap().term, "hello");
        assert_eq!(terms.next().unwrap().term, "happy");
        assert_eq!(terms.next().unwrap().term, "tax");
        assert_eq!(terms.next().unwrap().term, "payer");
        assert!(terms.next().is_none());
    }

    #[test]
    fn test_tokenizer_empty() {
        let mut terms = en_analyzer().analyze("");
        assert!(terms.next().is_none());
    }


    #[test]
    fn test_tokenizer_cjkchars() {
        let mut terms = en_analyzer().analyze("hello,中国人民");
        assert_eq!(terms.next().unwrap().term, "hello");
        assert_eq!(terms.next().unwrap().term, "中国人民");
        assert!(terms.next().is_none());
    }

}

