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


fn append_char(c: char, term_buffer: &mut String) {
    for c_lower in c.to_lowercase() {
        term_buffer.push(c_lower);
    }
}

impl<'a> TokenIter<'a> {


    pub fn read_one(&mut self, term_buffer: &mut String) -> bool {
        term_buffer.clear();
        loop {
            match self.chars.next() {
                Some(c) => {
                    if c.is_alphanumeric() {
                        append_char(c, term_buffer);
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
                        append_char(c, term_buffer);
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
