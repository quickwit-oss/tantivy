extern crate regex;

use self::regex::Regex;
use std::cell::RefCell;
use std::str::Chars;

lazy_static! {
    static ref WORD_PTN: Regex = Regex::new(r"[a-zA-Z0-9]+").unwrap();
}

pub struct TokenIter<'a> {
    chars: Chars<'a>,
}

impl<'a> TokenIter<'a> {
    pub fn read_one(&mut self, term_buffer: &mut String) -> bool {
        term_buffer.clear();
        loop {
            match self.chars.next() {
                Some(c) => {
                    if c.is_alphanumeric() {
                        term_buffer.push(c);
                        break;
                    }
                    else {
                        break;
                    }
                },
                None => {
                    return false;
                }
            }
        }
        loop {
            match self.chars.next() {
                Some(c) => {
                    if c.is_alphanumeric() {
                        term_buffer.push(c);
                    }
                    else {
                        break;
                    }
                },
                None => {
                    break;
                }
            }
        }
        return true;
    }
}

pub struct SimpleTokenizer;


impl SimpleTokenizer {
    pub fn new() -> SimpleTokenizer {
        SimpleTokenizer
    }

    pub fn tokenize<'a>(&self, text: &'a str) -> TokenIter<'a> {
       TokenIter {
           chars: text.chars(),
       }
   }
}
