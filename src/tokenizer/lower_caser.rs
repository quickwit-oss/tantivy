use super::{Token, TokenFilter, TokenStream};
use std::mem;

/// Token filter that lowercase terms.
#[derive(Clone)]
pub struct LowerCaser;

impl<TailTokenStream> TokenFilter<TailTokenStream> for LowerCaser
where
    TailTokenStream: TokenStream,
{
    type ResultTokenStream = LowerCaserTokenStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        LowerCaserTokenStream::wrap(token_stream)
    }
}

pub struct LowerCaserTokenStream<TailTokenStream> {
    buffer: String,
    tail: TailTokenStream,
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

impl<TailTokenStream> TokenStream for LowerCaserTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }

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
}

impl<TailTokenStream> LowerCaserTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn wrap(tail: TailTokenStream) -> LowerCaserTokenStream<TailTokenStream> {
        LowerCaserTokenStream {
            tail,
            buffer: String::with_capacity(100),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::LowerCaser;
    use crate::tokenizer::SimpleTokenizer;
    use crate::tokenizer::TokenStream;
    use crate::tokenizer::Tokenizer;

    #[test]
    fn test_to_lower_case() {
        assert_eq!(
            lowercase_helper("Русский текст"),
            vec!["русский".to_string(), "текст".to_string()]
        );
    }

    fn lowercase_helper(text: &str) -> Vec<String> {
        let mut tokens = vec![];
        let mut token_stream = SimpleTokenizer.filter(LowerCaser).token_stream(text);
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
