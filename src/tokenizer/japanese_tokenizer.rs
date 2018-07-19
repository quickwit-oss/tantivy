use super::{Token, TokenStream, Tokenizer};
use tinysegmenter;

/// Simple japanese tokenizer based on the `tinysegmenter` crate.
#[derive(Clone)]
pub struct JapaneseTokenizer;

#[derive(Eq, PartialEq)]
enum Cursor {
    HasNotStarted,
    Cursor(usize),
    Terminated,
}

pub struct JapaneseTokenizerStream {
    tokens: Vec<Token>,
    cursor: Cursor,
}

impl<'a> Tokenizer<'a> for JapaneseTokenizer {
    type TokenStreamImpl = JapaneseTokenizerStream;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        let mut tokens = vec![];
        let mut offset_from;
        let mut offset_to = 0;
        for (pos, term) in tinysegmenter::tokenize(text).into_iter().enumerate() {
            offset_from = offset_to;
            offset_to = offset_from + term.len();
            if term.chars().all(char::is_alphanumeric) {
                tokens.push(Token {
                    offset_from,
                    offset_to,
                    position: pos,
                    text: term,
                    position_length: 1
                });
            }
        }
        JapaneseTokenizerStream {
            tokens,
            cursor: Cursor::HasNotStarted,
        }
    }
}

impl<'a> TokenStream for JapaneseTokenizerStream {
    fn advance(&mut self) -> bool {
        let new_cursor = match self.cursor {
            Cursor::HasNotStarted => {
                if self.tokens.is_empty() {
                    Cursor::Terminated
                } else {
                    Cursor::Cursor(0)
                }
            }
            Cursor::Cursor(pos) => {
                let new_pos = pos + 1;
                if new_pos >= self.tokens.len() {
                    Cursor::Terminated
                } else {
                    Cursor::Cursor(new_pos)
                }
            }
            Cursor::Terminated => Cursor::Terminated,
        };
        self.cursor = new_cursor;
        self.cursor != Cursor::Terminated
    }

    fn token(&self) -> &Token {
        match self.cursor {
            Cursor::Terminated => {
                panic!("You called .token(), after the end of the token stream has been reached");
            }
            Cursor::Cursor(i) => &self.tokens[i],
            Cursor::HasNotStarted => {
                panic!("You called .token(), before having called `.advance()`.");
            }
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        match self.cursor {
            Cursor::Terminated => {
                panic!("You called .token(), after the end of the token stream has been reached");
            }
            Cursor::Cursor(i) => &mut self.tokens[i],
            Cursor::HasNotStarted => {
                panic!("You called .token(), before having called `.advance()`.");
            }
        }
    }
}
