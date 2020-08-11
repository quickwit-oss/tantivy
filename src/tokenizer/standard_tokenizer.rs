use super::BoxTokenStream;
use super::{Token, TokenStream, Tokenizer};
use std::str::Chars;

/// Tokenize the text by natural division.
#[derive(Clone)]
pub struct StandardTokenizer;

pub struct StandardTokenStream<'a> {
    text: &'a str,
    chars: Chars<'a>,
    front_offset: usize,
    pre_char: Option<char>,
    token: Token,
    len: usize,
}

impl Tokenizer for StandardTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(StandardTokenStream {
            text,
            chars: text.chars(),
            pre_char: None,
            front_offset: 0,
            len: text.len(),
            token: Token::default(),
        })
    }
}

impl<'a> StandardTokenStream<'a> {
    fn next(&mut self) -> (usize, usize) {
        let mut len = 0;
        let mut pre_type = 0;

        if let Some(c) = &self.pre_char {
            pre_type = Self::char_type(*c);
            len = c.len_utf8();
            match pre_type {
                4 => {
                    self.front_offset += len;
                    self.pre_char = None;
                    return (self.front_offset - len, self.front_offset);
                }
                _ => {}
            }
        }

        while let Some(c) = self.chars.next() {
            let tp = Self::char_type(c);

            match (pre_type, tp) {
                (0, 1) => {
                    self.front_offset += c.len_utf8();
                }
                (0, 2) | (0, 3) => {
                    len = c.len_utf8();
                    pre_type = tp;
                }
                (0, 4) => {
                    let c_len = c.len_utf8();
                    self.front_offset += c_len;
                    self.pre_char = None;
                    return (self.front_offset - c_len, self.front_offset);
                }
                (2, 1) | (3, 1) => {
                    let start = self.front_offset;
                    self.front_offset = self.front_offset + len + c.len_utf8();
                    self.pre_char = None;
                    return (start, start + len);
                }
                (2, 2) | (3, 3) => {
                    len += c.len_utf8();
                }
                _ => {
                    self.front_offset += len;
                    self.pre_char = Some(c);
                    return (self.front_offset - len, self.front_offset);
                }
            }
        }

        let start = self.front_offset;
        self.front_offset = self.len;
        self.pre_char = None;
        return (start, self.len);
    }

    /// return type for char
    /// 1: is alphabeti
    /// 2: is numeric
    /// 3: is write space
    /// 4: others
    pub fn char_type(c: char) -> usize {
        match c {
            ' ' => 1,
            'a'..='z' | 'A'..='Z' | 'Ａ'..='Ｚ' => 2,
            '0'..='9' | '０'..='９' => 3,
            _ => 4,
        }
    }
}

impl<'a> TokenStream for StandardTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        loop {
            let (begin, end) = self.next();
            if begin == self.len {
                return false;
            }
            self.token.offset_from = begin;
            self.token.offset_to = end;
            self.token.text.push_str(&self.text[begin..end]);
            return true;
        }
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[test]
fn test_standard_tokenizer() {
    let st = StandardTokenizer;

    let mut ts = st.token_stream("abcd1234 abc123 abc 123");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 0);
    assert_eq!(v.text, "abcd");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 4);
    assert_eq!(v.text, "1234");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 9);
    assert_eq!(v.text, "abc");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 12);
    assert_eq!(v.text, "123");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 16);
    assert_eq!(v.text, "abc");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 20);
    assert_eq!(v.text, "123");
    assert_eq!(true, ts.next().is_none());

    let mut ts = st.token_stream("我爱天安门abc123 sdf 456!@#$!@ 123长城！");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 0);
    assert_eq!(v.text, "我");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 3);
    assert_eq!(v.text, "爱");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 6);
    assert_eq!(v.text, "天");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 9);
    assert_eq!(v.text, "安");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 12);
    assert_eq!(v.text, "门");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 15);
    assert_eq!(v.text, "abc");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 18);
    assert_eq!(v.text, "123");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 22);
    assert_eq!(v.text, "sdf");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 26);
    assert_eq!(v.text, "456");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 29);
    assert_eq!(v.text, "!");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 30);
    assert_eq!(v.text, "@");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 31);
    assert_eq!(v.text, "#");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 32);
    assert_eq!(v.text, "$");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 33);
    assert_eq!(v.text, "!");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 34);
    assert_eq!(v.text, "@");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 36);
    assert_eq!(v.text, "123");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 39);
    assert_eq!(v.text, "长");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 42);
    assert_eq!(v.text, "城");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 45);
    assert_eq!(v.text, "！");
    assert_eq!(true, ts.next().is_none());

    assert_eq!(true, st.token_stream("").next().is_none());

    assert_eq!(true, st.token_stream("   ").next().is_none());

    let mut ts = st.token_stream("国 ");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 0);
    assert_eq!(v.offset_to, 3);
    assert_eq!(v.text, "国");
    assert_eq!(true, ts.next().is_none());

    let mut ts = st.token_stream(" 国");
    let v = ts.next().unwrap();
    assert_eq!(v.offset_from, 1);
    assert_eq!(v.offset_to, 4);
    assert_eq!(v.text, "国");
    assert_eq!(true, ts.next().is_none());
}
