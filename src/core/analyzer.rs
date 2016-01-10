extern crate regex;

use self::regex::Regex;

lazy_static! {
    static ref WORD_PTN: Regex = Regex::new(r"[a-zA-Z0-9]+").unwrap();
}


pub struct TokenIter<'a> {
    text: &'a str,
    token_it: Box<Iterator<Item=(usize, usize)> + 'a>,
}

impl<'a> Iterator for TokenIter<'a> {

    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        self.token_it.next().map(|(start, end)| &self.text[start..end])
    }

}

pub fn tokenize<'a>(text: &'a str)->TokenIter<'a> {
    TokenIter {
        text: text,
        token_it: Box::new(WORD_PTN.find_iter(text)),
    }
}
