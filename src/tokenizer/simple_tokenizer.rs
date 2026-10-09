use std::str::CharIndices;

use super::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting on whitespaces and punctuation.
#[derive(Clone, Default)]
pub struct SimpleTokenizer {
    token: Token,
}

/// TokenStream produced by the `SimpleTokenizer`.
pub struct SimpleTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
    // Pure ASCII text: scan bytes with a lookup table instead of decoding chars.
    // `ascii_pos` is the next byte to look at.
    ascii: bool,
    ascii_pos: usize,
}

/// `IS_ALNUM[b]` is `(b as char).is_alphanumeric()` for ASCII bytes. For ASCII, Unicode
/// alphanumeric is exactly `[0-9A-Za-z]`.
static IS_ASCII_ALNUM: [bool; 256] = {
    let mut table = [false; 256];
    let mut byte = 0;
    while byte < 128 {
        table[byte] = (byte as u8).is_ascii_alphanumeric();
        byte += 1;
    }
    table
};

impl Tokenizer for SimpleTokenizer {
    type TokenStream<'a> = SimpleTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> SimpleTokenStream<'a> {
        self.token.reset();
        SimpleTokenStream {
            text,
            chars: text.char_indices(),
            token: &mut self.token,
            ascii: text.is_ascii(),
            ascii_pos: 0,
        }
    }
}

impl SimpleTokenStream<'_> {
    /// Same tokens as the char path, for a pure ASCII text.
    #[inline]
    fn advance_ascii(&mut self) -> bool {
        let bytes = self.text.as_bytes();
        let mut pos = self.ascii_pos;
        while pos < bytes.len() && !IS_ASCII_ALNUM[bytes[pos] as usize] {
            pos += 1;
        }
        if pos == bytes.len() {
            self.ascii_pos = pos;
            return false;
        }
        let offset_from = pos;
        while pos < bytes.len() && IS_ASCII_ALNUM[bytes[pos] as usize] {
            pos += 1;
        }
        self.token.offset_from = offset_from;
        self.token.offset_to = pos;
        self.token.text.push_str(&self.text[offset_from..pos]);
        // The char path consumes the separator that ends a token.
        self.ascii_pos = (pos + 1).min(bytes.len());
        true
    }

    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| !c.is_alphanumeric())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl TokenStream for SimpleTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        if self.ascii {
            return self.advance_ascii();
        }
        while let Some((offset_from, c)) = self.chars.next() {
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::tests::assert_token;
    use crate::tokenizer::{SimpleTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_simple_tokenizer() {
        let tokens = token_stream_helper("Hello, happy tax payer!");
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "Hello", 0, 5);
        assert_token(&tokens[1], 1, "happy", 7, 12);
        assert_token(&tokens[2], 2, "tax", 13, 16);
        assert_token(&tokens[3], 3, "payer", 17, 22);
    }

    /// The char-based path, as it was before the ASCII fast path.
    fn reference_tokens(text: &str) -> Vec<(usize, usize, String)> {
        let mut tokens = Vec::new();
        let mut chars = text.char_indices();
        while let Some((offset_from, c)) = chars.next() {
            if c.is_alphanumeric() {
                let offset_to = chars
                    .find(|(_, c)| !c.is_alphanumeric())
                    .map(|(offset, _)| offset)
                    .unwrap_or(text.len());
                tokens.push((
                    offset_from,
                    offset_to,
                    text[offset_from..offset_to].to_string(),
                ));
            }
        }
        tokens
    }

    fn simple_tokens(text: &str) -> Vec<(usize, usize, String)> {
        token_stream_helper(text)
            .into_iter()
            .enumerate()
            .map(|(idx, token)| {
                assert_eq!(token.position, idx);
                (token.offset_from, token.offset_to, token.text)
            })
            .collect()
    }

    #[test]
    fn test_simple_tokenizer_ascii_and_unicode_paths() {
        for text in [
            "",
            " ",
            "a",
            "ab cd",
            "  leading and trailing  ",
            "GetCartAsync called with userId=9670e906-94eb-11f0",
            "failed to place order: rpc error: code = Unavailable",
            "x\u{1}y\u{7f}z",
            "héllo wörld",
            "日本語 text ١٢٣",
        ] {
            assert_eq!(simple_tokens(text), reference_tokens(text), "{text:?}");
        }
    }

    proptest::proptest! {
        #[test]
        fn test_simple_tokenizer_matches_char_path(text in "[ -~\t\n]{0,64}|.{0,32}") {
            proptest::prop_assert_eq!(simple_tokens(&text), reference_tokens(&text));
        }
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let mut a = TextAnalyzer::from(SimpleTokenizer::default());
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
