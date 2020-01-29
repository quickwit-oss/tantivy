use super::{Token, TokenFilter, TokenStream};
use crate::tokenizer::BoxTokenStream;
use std::mem;

impl TokenFilter for LowerCaser {
    fn transform<'a>(&self, token_stream: BoxTokenStream<'a>) -> BoxTokenStream<'a> {
        BoxTokenStream::from(LowerCaserTokenStream {
            tail: token_stream,
            buffer: String::with_capacity(100),
        })
    }
}

/// Token filter that lowercase terms.
#[derive(Clone)]
pub struct LowerCaser;

pub struct LowerCaserTokenStream<'a> {
    buffer: String,
    tail: BoxTokenStream<'a>,
}

// writes a lowercased version of text into output.
fn to_lowercase_unicode(text: &mut String, output: &mut String) {
    output.clear();
    for c in text.chars() {
        // Contrary to the std, we do not take care of sigma special case.
        // This will have an normalizationo effect, which is ok for search.
        output.extend(c.to_lowercase());
    }
}

impl<'a> TokenStream for LowerCaserTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        if self.token_mut().text.is_ascii() {
            // fast track for ascii.
            self.token_mut().text.make_ascii_lowercase();
        } else {
            to_lowercase_unicode(&mut self.tail.token_mut().text, &mut self.buffer);
            mem::swap(&mut self.tail.token_mut().text, &mut self.buffer);
        }
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
    use crate::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};

    #[test]
    fn test_to_lower_case() {
        assert_eq!(
            lowercase_helper("Русский текст"),
            vec!["русский".to_string(), "текст".to_string()]
        );
    }

    fn lowercase_helper(text: &str) -> Vec<String> {
        let mut tokens = vec![];
        let mut token_stream = TextAnalyzer::from(SimpleTokenizer)
            .filter(LowerCaser)
            .token_stream(text);
        while token_stream.advance() {
            let token_text = token_stream.token().text.clone();
            tokens.push(token_text);
        }
        tokens
    }

    #[test]
    fn test_lowercaser() {
        assert_eq!(lowercase_helper("Tree"), vec!["tree".to_string()]);
        assert_eq!(lowercase_helper("Русский"), vec!["русский".to_string()]);
    }
}
