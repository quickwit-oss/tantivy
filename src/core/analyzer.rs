
pub struct TokenIter<'a> {
    text: &'a String,
}

impl<'a> Iterator for TokenIter<'a> {

    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        None
    }

}

pub fn tokenize<'a>(text: &'a String)->TokenIter<'a> {
    TokenIter {
        text: text
    }
}
