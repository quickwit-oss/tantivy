use crate::tokenizer::{Token, TokenStream, TokenStreamChain};
use std::cmp::Ordering;

/// Struct representing pre-tokenized text
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PreTokenizedString {
    /// Original text
    pub text: String,
    /// Tokens derived from the text
    pub tokens: Vec<Token>,
}

impl Ord for PreTokenizedString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.text.cmp(&other.text)
    }
}

impl PartialOrd for PreTokenizedString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// TokenStream implementation which wraps PreTokenizedString
pub struct PreTokenizedStream {
    tokenized_string: PreTokenizedString,
    current_token: i64,
}

impl From<PreTokenizedString> for PreTokenizedStream {
    fn from(s: PreTokenizedString) -> PreTokenizedStream {
        PreTokenizedStream {
            tokenized_string: s,
            current_token: -1,
        }
    }
}

impl PreTokenizedStream {
    /// Creates a TokenStream from PreTokenizedString array
    pub fn chain_tokenized_strings<'a>(
        tok_strings: &'a [&'a PreTokenizedString],
    ) -> Box<dyn TokenStream + 'a> {
        if tok_strings.len() == 1 {
            Box::new(PreTokenizedStream::from((*tok_strings[0]).clone()))
        } else {
            let mut offsets = vec![];
            let mut total_offset = 0;
            for &tok_string in tok_strings {
                offsets.push(total_offset);
                let offset = match tok_string.tokens.last() {
                    Some(token) => token.offset_to,
                    None => 0,
                };
                total_offset += offset;
            }
            let token_streams: Vec<_> = tok_strings
                .iter()
                .map(|tok_string| PreTokenizedStream::from((*tok_string).clone()))
                .collect();
            Box::new(TokenStreamChain::new(offsets, token_streams))
        }
    }
}

impl TokenStream for PreTokenizedStream {
    fn advance(&mut self) -> bool {
        self.current_token += 1;
        self.current_token < self.tokenized_string.tokens.len() as i64
    }

    fn token(&self) -> &Token {
        if self.current_token < 0 {
            panic!("TokenStream not initialized. You should call advance() at least once.")
        }
        &self.tokenized_string.tokens[self.current_token as usize]
    }

    fn token_mut(&mut self) -> &mut Token {
        if self.current_token < 0 {
            panic!("TokenStream not initialized. You should call advance() at least once.")
        }
        &mut self.tokenized_string.tokens[self.current_token as usize]
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::tokenizer::Token;

    #[test]
    fn test_tokenized_stream() {
        let tok_text = PreTokenizedString {
            text: String::from("A a"),
            tokens: vec![
                Token {
                    offset_from: 0,
                    offset_to: 1,
                    position: 0,
                    text: String::from("A"),
                    position_length: 1,
                },
                Token {
                    offset_from: 2,
                    offset_to: 3,
                    position: 1,
                    text: String::from("a"),
                    position_length: 1,
                },
            ],
        };

        let mut tok_stream = PreTokenizedStream::from(tok_text.clone());

        let mut i = 0;
        while tok_stream.advance() {
            assert!(*tok_stream.token() == tok_text.tokens[i]);
            i += 1;
        }
    }

    #[test]
    fn test_chain_tokenized_strings() {
        let tok_text = PreTokenizedString {
            text: String::from("A a"),
            tokens: vec![
                Token {
                    offset_from: 0,
                    offset_to: 1,
                    position: 0,
                    text: String::from("A"),
                    position_length: 1,
                },
                Token {
                    offset_from: 2,
                    offset_to: 3,
                    position: 1,
                    text: String::from("a"),
                    position_length: 1,
                },
            ],
        };

        let chain_parts = vec![&tok_text, &tok_text];

        let mut tok_stream = PreTokenizedStream::chain_tokenized_strings(&chain_parts[..]);

        let expected_tokens = vec![
            Token {
                offset_from: 0,
                offset_to: 1,
                position: 0,
                text: String::from("A"),
                position_length: 1,
            },
            Token {
                offset_from: 2,
                offset_to: 3,
                position: 1,
                text: String::from("a"),
                position_length: 1,
            },
            Token {
                offset_from: 3,
                offset_to: 4,
                position: 3,
                text: String::from("A"),
                position_length: 1,
            },
            Token {
                offset_from: 5,
                offset_to: 6,
                position: 4,
                text: String::from("a"),
                position_length: 1,
            },
        ];
        let mut i = 0;
        while tok_stream.advance() {
            assert!(*tok_stream.token() == expected_tokens[i]);
            i += 1;
        }
    }
}
