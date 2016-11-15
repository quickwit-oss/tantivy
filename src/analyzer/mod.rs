extern crate regex;

use std::str::Chars;
use std::ascii::AsciiExt;

pub struct TokenIter<'a> {
    chars: Chars<'a>,
    term_buffer: String,
}

fn append_char_lowercase(c: char, term_buffer: &mut String) {
    term_buffer.push(c.to_ascii_lowercase());
}

pub trait StreamingIterator<'a, T> {
    fn next(&'a mut self) -> Option<T>;
}

impl<'a, 'b> TokenIter<'b> {
    fn consume_token(&'a mut self) -> Option<&'a str> {
        for c in &mut self.chars { 
            if c.is_alphanumeric() {
                append_char_lowercase(c, &mut self.term_buffer);
            }
            else {
                break;
            }
        }
        Some(&self.term_buffer)
    }
}


impl<'a, 'b> StreamingIterator<'a, &'a str> for TokenIter<'b> {
    
    #[inline]
    fn next(&'a mut self,) -> Option<&'a str> {
        self.term_buffer.clear();
        // skipping non-letter characters.
        loop {
            match self.chars.next() {
                Some(c) => {
                    if c.is_alphanumeric() {
                        append_char_lowercase(c, &mut self.term_buffer);
                        return self.consume_token();
                    }
                }
                None => { return None; }
            }
        }
    }
    
}

pub struct SimpleTokenizer;


impl SimpleTokenizer {

    pub fn tokenize<'a>(&self, text: &'a str) -> TokenIter<'a> {
        TokenIter {
           term_buffer: String::new(),
           chars: text.chars(),
        }
   }
}


#[test]
fn test_tokenizer() {
    let simple_tokenizer = SimpleTokenizer;
    let mut term_reader = simple_tokenizer.tokenize("hello, happy tax payer!");
    assert_eq!(term_reader.next().unwrap(), "hello");
    assert_eq!(term_reader.next().unwrap(), "happy");
    assert_eq!(term_reader.next().unwrap(), "tax");
    assert_eq!(term_reader.next().unwrap(), "payer");
    assert_eq!(term_reader.next(), None);
}


#[test]
fn test_tokenizer_empty() {
    let simple_tokenizer = SimpleTokenizer;
    let mut term_reader = simple_tokenizer.tokenize("");
    assert_eq!(term_reader.next(), None);
}
