use super::{BoxTokenStream, Token, TokenFilter, TokenStream};

#[derive(Clone)]
pub struct FixedShingleFilter {
    size: usize,
    separator: char,
}

impl FixedShingleFilter {
    pub fn new(size: usize) -> Self {
        FixedShingleFilter::new_with_separator(size, '_')
    }

    pub fn new_with_separator(size: usize, separator: char) -> Self {
        FixedShingleFilter { size, separator }
    }
}

impl TokenFilter for FixedShingleFilter {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        BoxTokenStream::from(ShingleFilterStream {
            size: self.size,
            separator: self.separator,
            buffer: String::with_capacity(100),
            pendings: Vec::with_capacity(self.size - 1),
            tail: token_stream,
            original_token_position: 0,
        })
    }
}

pub struct ShingleFilterStream<'a> {
    size: usize,
    separator: char,
    buffer: String,
    pendings: Vec<Token>,
    tail: BoxTokenStream<'a>,
    original_token_position: usize,
}

impl<'a> ShingleFilterStream<'a> {
    fn is_window_filled(&self) -> bool {
        self.pendings.len() == self.size - 1
    }

    fn push_token(&mut self, mut token: Token) {
        if self.is_window_filled() {
            self.pendings.remove(0);
        }
        token.position = self.original_token_position;
        self.pendings.push(token);
    }
}

fn make_shingle(token: &mut Token, pendings: &[Token], buffer: &mut String, separator: char) {
    buffer.clear();
    buffer.push_str(token.text.as_str());

    // set text
    token.text.clear();
    for pend_token in pendings {
        token.text.push_str(pend_token.text.as_str());
        token.text.push(separator);
    }
    token.text.push_str(buffer.as_str());

    // set offset and position
    if let Some(first_token) = pendings.get(0) {
        token.offset_from = first_token.offset_from;
        token.position = first_token.position;
    }
}

impl<'a> TokenStream for ShingleFilterStream<'a> {
    fn advance(&mut self) -> bool {
        while !self.is_window_filled() {
            if !self.tail.advance() {
                return false;
            }
            self.push_token(self.tail.token().clone());
            self.original_token_position += 1;  // This is not compatible with StopWordFilter
        }
        if !self.tail.advance() {
            return false;
        }
        let next_token = self.tail.token().clone();
        make_shingle(
            &mut self.tail.token_mut(),
            &self.pendings,
            &mut self.buffer,
            self.separator,
        );
        self.push_token(next_token);
        self.original_token_position += 1;
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::{FixedShingleFilter, SimpleTokenizer, TextAnalyzer, Token};

    #[test]
    fn test_shingle_bigram() {
        let tokens = shingle_helper("i am a cat", 2);
        let expected = vec![
            Token {
                text: "i_am".to_string(),
                offset_from: 0,
                offset_to: 4,
                position: 0,
                position_length: 1,
            },
            Token {
                text: "am_a".to_string(),
                offset_from: 2,
                offset_to: 6,
                position: 1,
                position_length: 1,
            },
            Token {
                text: "a_cat".to_string(),
                offset_from: 5,
                offset_to: 10,
                position: 2,
                position_length: 1,
            },
        ];
        assert_eq!(&tokens, &expected);
    }

    #[test]
    fn test_shingle_trigram() {
        let tokens = shingle_helper("as yet i have no name", 3);
        let expected = vec![
            Token {
                text: "as_yet_i".to_string(),
                offset_from: 0,
                offset_to: 8,
                position: 0,
                position_length: 1,
            },
            Token {
                text: "yet_i_have".to_string(),
                offset_from: 3,
                offset_to: 13,
                position: 1,
                position_length: 1,
            },
            Token {
                text: "i_have_no".to_string(),
                offset_from: 7,
                offset_to: 16,
                position: 2,
                position_length: 1,
            },
            Token {
                text: "have_no_name".to_string(),
                offset_from: 9,
                offset_to: 21,
                position: 3,
                position_length: 1,
            },
        ];
        assert_eq!(&tokens, &expected);
    }

    fn shingle_helper(text: &str, size: usize) -> Vec<Token> {
        let mut tokens = vec![];
        let mut token_stream = TextAnalyzer::from(SimpleTokenizer)
            .filter(FixedShingleFilter::new(size))
            .token_stream(text);
        while token_stream.advance() {
            let token_next = token_stream.token().clone();
            tokens.push(token_next);
        }
        tokens
    }

    pub fn assert_token(token: &Token, position: usize, text: &str, from: usize, to: usize) {
        assert_eq!(
            token.position, position,
            "expected position {} but {:?}",
            position, token
        );
        assert_eq!(token.text, text, "expected text {} but {:?}", text, token);
        assert_eq!(
            token.offset_from, from,
            "expected offset_from {} but {:?}",
            from, token
        );
        assert_eq!(
            token.offset_to, to,
            "expected offset_to {} but {:?}",
            to, token
        );
    }
}
