use regex::{CaptureLocations, Error, Match, Regex};

use super::{BoxTokenStream, Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting based on the captures of a regular expression:
/// - if no capture groups are specified, all matches of the entire expression will be tokenized
/// - if capture groups are specified, their matches will instead be tokenized.
/// Note that this deviates from what one might expect from regex captures, which usually include
/// the entire matched expression as well (as capture group 0). However, since there is no way to
/// filter these out later, the `RegexTokenizer` discards them itself. If matching of the entire
/// expression is desired, be sure to include a capture group around the entire expression.
///
/// # Simple example (no capture groups)
///
/// ```rust
/// use tantivy::tokenizer::*;
///
/// let tokenizer = RegexTokenizer::new_from_pattern(r"\d{3}\w*").unwrap();
/// let mut stream = tokenizer.token_stream("1 12 123 1234");
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "123");
///     assert_eq!(token.offset_from, 5);
///     assert_eq!(token.offset_to, 8);
/// }
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "1234");
///     assert_eq!(token.offset_from, 9);
///     assert_eq!(token.offset_to, 13);
/// }
/// assert!(stream.next().is_none());
/// ```
///
/// # Capture groups example
///
/// ```rust
/// use tantivy::tokenizer::*;
///
/// let tokenizer = RegexTokenizer::new_from_pattern(r"([a-z0-9_\.\+-]+)@([\da-z\.-]+)\.([a-z\.]{2,6})").unwrap();
/// let mut stream = tokenizer.token_stream("jane.doe@example.com john.doe@example.com @invalid.example.com");
/// {
///     let token = stream.next().unwrap();
///     assert_eq!(token.text, "jane.doe");
///     assert_eq!(token.offset_from, 0);
///     assert_eq!(token.offset_to, 8);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "example");
///     assert_eq!(token.offset_from, 9);
///     assert_eq!(token.offset_to, 16);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "com");
///     assert_eq!(token.offset_from, 17);
///     assert_eq!(token.offset_to, 20);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "john.doe");
///     assert_eq!(token.offset_from, 21);
///     assert_eq!(token.offset_to, 29);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "example");
///     assert_eq!(token.offset_from, 30);
///     assert_eq!(token.offset_to, 37);
/// }
/// {
///   let token = stream.next().unwrap();
///     assert_eq!(token.text, "com");
///     assert_eq!(token.offset_from, 38);
///     assert_eq!(token.offset_to, 41);
/// }
/// assert!(stream.next().is_none());
/// ```
#[derive(Clone)]
pub struct RegexTokenizer {
    regex: Regex,
}

impl RegexTokenizer {
    /// Configures a new `RegexTokenizer` using the given `Regex`
    pub fn new(regex: Regex) -> RegexTokenizer {
        RegexTokenizer { regex }
    }
    /// Attempts to configure a new `RegexTokenizer` based on the provided pattern
    pub fn new_from_pattern(pattern: &str) -> Result<RegexTokenizer, Error> {
        Ok(RegexTokenizer {
            regex: Regex::new(pattern)?,
        })
    }
}

/// TokenStream associate to the `RegexTokenizer`
pub struct RegexTokenStream<'a> {
    /// input
    text: &'a str,
    /// Regular expression to match
    regex: Regex,
    /// Current match of the regular expression
    regex_match: Option<Match<'a>>,
    /// index of the current capture group
    capture_index: usize,
    /// locations of all matched capture groups
    capture_locations: Option<CaptureLocations>,
    /// output
    token: Token,
}

impl Tokenizer for RegexTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(RegexTokenStream::new(text, &self.regex))
    }
}

impl<'a> RegexTokenStream<'a> {
    fn new(text: &'a str, regex: &Regex) -> RegexTokenStream<'a> {
        let capture_locations = if regex.captures_len() > 1 {
            Some(regex.capture_locations())
        } else {
            None
        };
        RegexTokenStream {
            text,
            regex: regex.clone(),
            regex_match: None,
            capture_locations,
            capture_index: 0,
            token: Token::default(),
        }
    }
    /// Attempts to advance the [`RegexTokenStream`] to the next capture group within the same match
    /// of the regex.
    /// Returns `true` if and only if a current match is present and contains another valid capture
    /// group and sets the token accordingly.
    /// Will panic if `self.capture_locations` is None.
    fn next_capture_group(&mut self) -> bool {
        self.capture_index = self.capture_index + 1;
        self.capture_locations
            .as_ref()
            .unwrap()
            .get(self.capture_index)
            .map_or(false, |(group_start, group_end)| {
                self.token.text.clear();
                self.token.position = self.token.position.wrapping_add(1);
                self.token.text.push_str(&self.text[group_start..group_end]);
                self.token.offset_from = group_start;
                self.token.offset_to = group_end;
                return true;
            })
    }

    /// Attempts to advance the [`RegexTokenStream`] to the next match of the regex using
    /// `captures_read_at`. Will panic if `start` is out of bounds.
    fn next_captures(&mut self, start: usize) -> bool {
        self.regex
            .captures_read_at(
                &mut self.capture_locations.as_mut().unwrap(),
                &self.text,
                start,
            )
            .map_or(false, |regex_match| {
                self.regex_match = Some(regex_match);
                self.capture_index = 0;
                true
            })
    }

    /// Attempts to advance the [`RegexTokenStream`] to the next match of the regex using the simple
    /// (and more performant) `find_at`. Will panic if `start` is out of bounds.
    fn next_match(&mut self, start: usize) -> bool {
        self.regex
            .find_at(self.text, start)
            .map_or(false, |regex_match| {
                self.token.text.clear();
                self.token.position = self.token.position.wrapping_add(1);
                self.token.offset_from = regex_match.start();
                self.token.offset_to = regex_match.end();
                self.token
                    .text
                    .push_str(&self.text[self.token.offset_from..self.token.offset_to]);
                self.regex_match = Some(regex_match);
                true
            })
    }
}

impl<'a> TokenStream for RegexTokenStream<'a> {
    fn advance(&mut self) -> bool {
        let tail_index = self.regex_match.map_or(0, |regex_match| regex_match.end());
        if self.capture_locations.is_some() {
            while !self.next_capture_group() {
                if tail_index > self.text.len() || !self.next_captures(tail_index) {
                    return false;
                }
            }
            true
        } else {
            tail_index < self.text.len() && self.next_match(tail_index)
        }
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
    use super::RegexTokenizer;
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{TextAnalyzer, Token};

    #[test]
    fn test_regex_tokenizer_no_capture() {
        let tokens = token_stream_helper("1+2 3+4+5\n6+7++89", r"\++\d+");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 0, "+2", 1, 3);
        assert_token(&tokens[1], 1, "+4", 5, 7);
        assert_token(&tokens[2], 2, "+5", 7, 9);
        assert_token(&tokens[3], 3, "+7", 11, 13);
        assert_token(&tokens[4], 4, "++89", 13, 17);
    }

    #[test]
    fn test_regex_tokenizer_single_capture() {
        let tokens = token_stream_helper(
            "1 2 3 1815-10-10 10-11-12 2023-01-03 12345-67-89",
            r"\s(\d{4}-\d{2}-\d{2})\s",
        );
        assert_eq!(tokens.len(), 2);
        assert_token(&tokens[0], 0, "1815-10-10", 6, 16);
        assert_token(&tokens[1], 1, "2023-01-03", 26, 36);
    }

    #[test]
    fn test_regex_tokenizer_nested_capture() {
        let tokens = token_stream_helper(
            "1 2 3 1815-10-10 10-11-12 1933-04-23 12345-67-89",
            r"\s((\d{4})-\d{2}-\d{2})\s",
        );
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "1815-10-10", 6, 16);
        assert_token(&tokens[1], 1, "1815", 6, 10);
        assert_token(&tokens[2], 2, "1933-04-23", 26, 36);
        assert_token(&tokens[3], 3, "1933", 26, 30);
    }

    #[test]
    fn test_regex_tokenizer_multi_capture() {
        let tokens = token_stream_helper(
            "jane.doe@example.com john.doe@example.com @invalid.example.com",
            r"([a-z0-9_\.\+-]+)@([\da-z\.-]+)\.([a-z\.]{2,6})",
        );
        assert_eq!(tokens.len(), 6);
        assert_token(&tokens[0], 0, "jane.doe", 0, 8);
        assert_token(&tokens[1], 1, "example", 9, 16);
        assert_token(&tokens[2], 2, "com", 17, 20);
        assert_token(&tokens[3], 3, "john.doe", 21, 29);
        assert_token(&tokens[4], 4, "example", 30, 37);
        assert_token(&tokens[5], 5, "com", 38, 41);
    }

    #[test]
    fn test_regex_tokenizer_partial_capture() {
        let tokens = token_stream_helper("RegEx is FUN!", r"((\b[A-Z])*\w+)");
        assert_eq!(tokens.len(), 5);
        assert_token(&tokens[0], 0, "RegEx", 0, 5);
        assert_token(&tokens[1], 1, "R", 0, 1);
        assert_token(&tokens[2], 2, "is", 6, 8);
        assert_token(&tokens[3], 3, "FUN", 9, 12);
        assert_token(&tokens[4], 4, "F", 9, 10);
    }

    fn token_stream_helper(text: &str, pattern: &str) -> Vec<Token> {
        let a = TextAnalyzer::from(RegexTokenizer::new_from_pattern(pattern).unwrap());
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
