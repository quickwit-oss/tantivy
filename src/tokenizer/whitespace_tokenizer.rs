use std::str::CharIndices;

use super::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting on whitespaces.
#[derive(Clone, Default)]
pub struct WhitespaceTokenizer {
    token: Token,
}

pub struct WhitespaceTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
}

impl Tokenizer for WhitespaceTokenizer {
    type TokenStream<'a> = WhitespaceTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> WhitespaceTokenStream<'a> {
        self.token.reset();
        WhitespaceTokenStream {
            text,
            chars: text.char_indices(),
            token: &mut self.token,
        }
    }
}

impl<'a> WhitespaceTokenStream<'a> {
    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| c.is_whitespace())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl<'a> TokenStream for WhitespaceTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if !c.is_whitespace() {
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
    use crate::tokenizer::{TextAnalyzer, Token, WhitespaceTokenizer};

    #[test]
    fn test_whitespace_tokenizer() {
        let tokens = token_stream_helper("Hello, happy tax payer!");
        assert_eq!(tokens.len(), 4);
        assert_token(&tokens[0], 0, "Hello,", 0, 6);
        assert_token(&tokens[1], 1, "happy", 7, 12);
        assert_token(&tokens[2], 2, "tax", 13, 16);
        assert_token(&tokens[3], 3, "payer!", 17, 23);
    }

    #[test]
    fn test_unicode_whitespaces() {
        let tokens = token_stream_helper("わ |　か　　花");
        assert_eq!(tokens.len(), 4);

        assert_eq!('わ'.len_utf8(), 3);
        assert_token(&tokens[0], 0, "わ", 0, 3);

        assert_eq!('|'.len_utf8(), 1);
        assert_token(&tokens[1], 1, "|", 4, 5);

        assert_eq!('　'.len_utf8(), 3);

        assert_eq!('か'.len_utf8(), 3);
        assert_token(&tokens[2], 2, "か", 8, 11);

        assert_eq!('花'.len_utf8(), 3);
        assert_token(&tokens[3], 3, "花", 17, 20);
    }

    fn token_stream_helper(text: &str) -> Vec<Token> {
        let mut a = TextAnalyzer::from(WhitespaceTokenizer::default());
        let mut token_stream = a.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        let mut add_token = |token: &Token| {
            tokens.push(token.clone());
        };
        token_stream.process(&mut add_token);
        tokens
    }
}
