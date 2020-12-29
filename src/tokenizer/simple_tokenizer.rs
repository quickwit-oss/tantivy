use super::{Token, TokenStream, Tokenizer};
use std::str::CharIndices;

/// Tokenize the text by splitting on whitespaces and punctuation.
#[derive(Clone, Debug)]
pub struct SimpleTokenizer;

#[derive(Clone, Debug)]
pub struct SimpleTokenizerStream {
    text: String,
    idx: usize,
    chars: Vec<(usize, char)>,
    token: Token,
}

impl Tokenizer for SimpleTokenizer {
    type Iter = SimpleTokenizerStream;
    fn token_stream(&self, text: &str) -> Self::Iter {
        SimpleTokenizerStream {
            text: text.to_string(),
            chars: text.char_indices().collect(),
            idx: 0,
            token: Token::default(),
        }
    }
}

impl SimpleTokenizerStream {
    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .iter()
            .filter(|&&(_, ref c)| !c.is_alphanumeric())
            .map(|(offset, _)| *offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }
}

impl Iterator for SimpleTokenizerStream {
    type Item = Token;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.chars.len() {
            return None;
        }
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while self.idx < self.chars.len() {
            let (offset_from, c) = self.chars[self.idx];
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return Some(self.token.clone());
            }
            self.idx += 1;
        }
        None
    }
}

impl TokenStream for SimpleTokenizerStream {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_tokenizer() {
        let mut stream = SimpleTokenizer.token_stream("tokenizer hello world");
        dbg!(stream.next());
        dbg!(stream.next());
        dbg!(stream.next());
    }
}
