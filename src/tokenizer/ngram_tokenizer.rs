use super::{Token, TokenStream, Tokenizer};

/// Tokenize the text by splitting words into n-grams of the given size(s)
///
/// With this tokenizer, the `position` field expresses the starting offset of the ngram
/// rather than the `token` offset.
///
/// Example 1: `hello` would be tokenized as (min_gram: 2, max_gram: 3, prefix_only: false)
///
/// | Term     | he  | hel | el  | ell | ll  | llo | lo |
/// |----------|-----|-----|-----|-----|-----|-----|----|
/// | Position | 0   | 0   | 1   | 1   | 2   | 2   | 3  |
/// | Offsets  | 0,2 | 0,3 | 1,3 | 1,4 | 2,4 | 2,5 | 3,5|
///
/// Example 2: `hello` would be tokenized as (min_gram: 2, max_gram: 5, prefix_only: **true**)
///
/// | Term     | he  | hel | hell  | hello |
/// |----------|-----|-----|-------|-------|
/// | Position | 0   | 0   | 0     | 0     |
/// | Offsets  | 0,2 | 0,3 | 0,4   | 0,5   |
///
/// Example 3: `hεllo` (non-ascii) would be tokenized as (min_gram: 2, max_gram: 5, prefix_only: **true**)
///
/// | Term     | hε  | hεl | hεll  | hεllo |
/// |----------|-----|-----|-------|-------|
/// | Position | 0   | 0   | 0     | 0     |
/// | Offsets  | 0,3 | 0,4 | 0,5   | 0,6   |
///
/// # Example
///
/// ```
/// extern crate tantivy;
/// use tantivy::tokenizer::*;
/// use tantivy::tokenizer::assert_token;
///
/// # fn main() {
/// let tokenizer = NgramTokenizer::new(2, 3, false);
/// let mut stream = tokenizer.token_stream("hello");
///
/// assert_token(stream.next().unwrap(), 0, "he", 0, 2);
/// assert_token(stream.next().unwrap(), 0, "hel", 0, 3);
/// assert_token(stream.next().unwrap(), 1, "el", 1, 3);
/// assert_token(stream.next().unwrap(), 1, "ell", 1, 4);
/// assert_token(stream.next().unwrap(), 2, "ll", 2, 4);
/// assert_token(stream.next().unwrap(), 2, "llo", 2, 5);
/// assert_token(stream.next().unwrap(), 3, "lo", 3, 5);
/// assert!(stream.next().is_none());
/// # }
/// ```
#[derive(Clone)]
pub struct NgramTokenizer {
    /// min size of the n-gram
    min_gram: usize,
    /// max size of the n-gram
    max_gram: usize,
    /// if true, will only parse the leading edge of the input
    prefix_only: bool,
}

impl NgramTokenizer {
    /// Configures a new Ngram tokenizer
    pub fn new(min_gram: usize, max_gram: usize, prefix_only: bool) -> NgramTokenizer {
        assert!(min_gram > 0, "min_gram must be greater than 0");
        assert!(
            min_gram <= max_gram,
            "min_gram must not be greater than max_gram"
        );

        NgramTokenizer {
            min_gram,
            max_gram,
            prefix_only,
        }
    }
}
pub struct NgramTokenStream<'a> {
    text: &'a str,
    bytes: &'a [u8],
    bytes_length: usize,
    position: usize,
    token: Token,
    min_gram: usize,
    max_gram: usize,
    gram_size: usize,
    prefix_only: bool,
}

impl<'a> Tokenizer<'a> for NgramTokenizer {
    type TokenStreamImpl = NgramTokenStream<'a>;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        NgramTokenStream {
            text,
            bytes: text.as_bytes(),
            bytes_length: text.as_bytes().len(),
            position: 0,
            token: Token::default(),
            min_gram: self.min_gram,
            max_gram: self.max_gram,
            prefix_only: self.prefix_only,
            gram_size: self.min_gram,
        }
    }
}

impl<'a> NgramTokenStream<'a> {
    /// Get the next set of token options
    /// cycle through 1,2 (min..=max)
    /// returning None if processing should stop
    fn chomp(&mut self) -> Option<(usize, usize)> {
        // Have we exceeded the bounds of the text we are indexing?

        if self.gram_size > self.max_gram {
            if self.prefix_only {
                return None;
            }

            // since we aren't just processing edges
            // we need to reset the gram size
            self.gram_size = self.min_gram;

            // and move down the chain of letters (respecting multi-byte chars)
            let char_width = utf8_char_width(self.bytes[self.position]);

            self.position += char_width;
        }

        // convert gram_size into bytes
        // TODO: how could this be better?
        let mut size = 0;
        for _ in 0..self.gram_size {
            let next_char_index = self.position + size;
            if next_char_index >= self.bytes_length {
                // reset - this has gotten too big
                size = 0;
                // exit the loop
                break;
            } else {
                let byte = self.bytes[next_char_index];
                let width = utf8_char_width(byte);
                size += width;
            }
        }

        let result = if size > 0 && (self.position + size) <= self.bytes_length {
            Some((self.position, size))
        } else {
            None
        };

        self.gram_size += 1;

        result
    }
}

impl<'a> TokenStream for NgramTokenStream<'a> {
    fn advance(&mut self) -> bool {
        // clear out working token text
        self.token.text.clear();

        if let Some((position, size)) = self.chomp() {
            self.token.position = self.position;
            let offset_from = position;
            let offset_to = offset_from + size;

            self.token.offset_from = offset_from;
            self.token.offset_to = offset_to;

            let a = &self.text[offset_from..offset_to];
            self.token.text.push_str(a);

            true
        } else {
            false
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
mod test {
    #[test]
    fn multi_byte() {
        use super::utf8_char_width;

        let multi = "ε";
        let bytes = multi.as_bytes();
        assert_eq!(bytes.len(), 2);
        assert_eq!(utf8_char_width(bytes[0]), 2);
    }
}

static UTF8_CHAR_WIDTH: [u8; 256] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x1F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x3F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x5F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, // 0x7F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, // 0x9F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, // 0xBF
    0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, // 0xDF
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xEF
    4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xFF
];

#[inline]
pub fn utf8_char_width(b: u8) -> usize {
    unsafe { *UTF8_CHAR_WIDTH.get_unchecked(b as usize) as usize }
}
