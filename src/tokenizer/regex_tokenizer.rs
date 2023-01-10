use regex::Regex;
use crate::TantivyError;

use super::{BoxTokenStream, Token, TokenStream, Tokenizer};

/// Tokenize the text by using a regex pattern to split.
/// Each match of the regex emits a distinct token, empty tokens will not be emitted. Anchors such
/// as `\A` will match the text from the part where the last token was emitted or the beginning of
/// the complete text if no token was emitted yet.
///
/// Example: `` 'aaa' bbb 'ccc' 'ddd' `` with the pattern `` '(?:\w*)' `` will be tokenized as followed:
///
/// | Term     | aaa  | ccc    | ddd   |
/// |----------|------|--------|-------|
/// | Position | 1    | 2      | 3     |
/// | Offsets  |0,5   | 10,15  | 16,21 |
///
///
/// # Example
///
/// ```rust
/// use tantivy::tokenizer::*;
///
/// let tokenizer = RegexTokenizer::new(r"'(?:\w*)'");
/// let mut stream = tokenizer.token_stream("'aaa' bbb 'ccc' 'ddd'");
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "'aaa'");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 5);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "'ccc'");
///     assert_eq!(token.offset_from, 10);
///     assert_eq!(token.offset_to, 15);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "'ddd'");
///     assert_eq!(token.offset_from, 16);
///     assert_eq!(token.offset_to, 21);
/// }
/// assert!(stream.next().is_none());
/// ```

#[derive(Clone)]
pub struct RegexTokenizer {
    regex: Regex
}

impl RegexTokenizer {
    pub fn new(regex_pattern: &str) -> crate::Result<RegexTokenizer> {
        Regex::new(regex_pattern)
            .map_err(|_| TantivyError::InvalidArgument(regex_pattern.to_owned()))
            .map(|regex| Self { regex })
    }
}

impl Tokenizer for RegexTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(RegexTokenStream {
            regex: self.regex.clone(),
            text,
            token: Token::default(),
        })
    }
}

pub struct RegexTokenStream<'a> {
    regex: Regex,
    text: &'a str,
    token: Token,
}

impl<'a> TokenStream for RegexTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if let Some(m) = self.regex.find(self.text) {
            if !m.as_str().is_empty() {
                self.token.text.clear();
                self.token.text.push_str(&self.text[m.start()..m.end()]);

                self.token.offset_from = self.token.offset_to + m.start();
                self.token.offset_to = self.token.offset_to + m.end();

                self.token.position = self.token.position.wrapping_add(1);

                self.text = &self.text[m.end()..];
                return true
            }
        }
        false
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{TextAnalyzer, Token};
    use crate::tokenizer::regex_tokenizer::RegexTokenizer;
    
    #[test]
    fn test_regex_tokenizer() {
        let tokens = token_stream_helper("'aaa' bbb 'ccc' 'ddd'", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 3);
        assert_token(&tokens[0], 0, "'aaa'", 0, 5);
        assert_token(&tokens[1], 1, "'ccc'", 10, 15);
        assert_token(&tokens[2], 2, "'ddd'", 16, 21);
    }

    #[test]
    fn test_regexp_tokenizer_no_match_on_input_data() {
        let tokens = token_stream_helper("aaa", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_regexp_tokenizer_no_input_data() {
        let tokens = token_stream_helper("", r"'(?:\w*)'");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_regexp_tokenizer_error_on_invalid_regex() {
        let tokenizer = RegexTokenizer::new(r"\@");
        assert_eq!(tokenizer.is_err(), true);
        assert_eq!(tokenizer.err().unwrap().to_string(), "An invalid argument was passed: '\\@'");
    }

    fn token_stream_helper(text: &str, pattern: &str) -> Vec<Token> {
        let r = RegexTokenizer::new(pattern).unwrap();
        let a = TextAnalyzer::from(r);
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}