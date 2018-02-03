use super::{Token, TokenStream, Tokenizer};
use std::str;
use schema::FACET_SEP_BYTE;

/// The `FacetTokenizer` process a `Facet` binary representation
/// and emits a token for all of its parent.
///
/// For instance,  `/america/north_america/canada`
/// will emit the three following tokens
///     - `/america/north_america/canada`
///     - `/america/north_america`
///     - `/america`
#[derive(Clone)]
pub struct FacetTokenizer;

#[derive(Debug)]
enum State {
    RootFacetNotEmitted,
    UpToPosition(usize), //< we already emitted facet prefix up to &text[..cursor]
    Terminated,
}

pub struct FacetTokenStream<'a> {
    text: &'a str,
    state: State,
    token: Token,
}

impl<'a> Tokenizer<'a> for FacetTokenizer {
    type TokenStreamImpl = FacetTokenStream<'a>;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        FacetTokenStream {
            text: text,
            state: State::RootFacetNotEmitted, //< pos is the first char that has not been processed yet.
            token: Token::default(),
        }
    }
}

impl<'a> TokenStream for FacetTokenStream<'a> {
    fn advance(&mut self) -> bool {
        match self.state {
            State::RootFacetNotEmitted => {
                self.state = if self.text.is_empty() {
                    State::Terminated
                } else {
                    State::UpToPosition(0)
                };
                true
            }
            State::UpToPosition(cursor) => {
                let bytes: &[u8] = self.text.as_bytes();
                if let Some(next_sep_pos) = bytes[cursor + 1..]
                    .iter()
                    .cloned()
                    .position(|b| b == FACET_SEP_BYTE)
                    .map(|pos| cursor + 1 + pos)
                {
                    let facet_part =
                        unsafe { str::from_utf8_unchecked(&bytes[cursor..next_sep_pos]) };
                    self.token.text.push_str(facet_part);
                    self.state = State::UpToPosition(next_sep_pos);
                } else {
                    let facet_part = unsafe { str::from_utf8_unchecked(&bytes[cursor..]) };
                    self.token.text.push_str(facet_part);
                    self.state = State::Terminated;
                }
                true
            }
            State::Terminated => false,
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
mod tests {

    use tokenizer::{Token, TokenStream, Tokenizer};
    use super::FacetTokenizer;
    use schema::Facet;

    #[test]
    fn test_facet_tokenizer() {
        let facet = Facet::from_path(vec!["top", "a", "b"]);
        let mut tokens = vec![];
        {
            let mut add_token = |token: &Token| {
                let facet = Facet::from_encoded(token.text.as_bytes().to_owned());
                tokens.push(format!("{}", facet));
            };
            FacetTokenizer
                .token_stream(unsafe {
                    ::std::str::from_utf8_unchecked(facet.encoded_bytes())
                })
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], "/");
        assert_eq!(tokens[1], "/top");
        assert_eq!(tokens[2], "/top/a");
        assert_eq!(tokens[3], "/top/a/b");
    }

    #[test]
    fn test_facet_tokenizer_root_facets() {
        let facet = Facet::root();
        let mut tokens = vec![];
        {
            let mut add_token = |token: &Token| {
                let facet = Facet::from_encoded(token.text.as_bytes().to_owned());
                tokens.push(format!("{}", facet));
            };
            FacetTokenizer
                .token_stream(unsafe {
                    ::std::str::from_utf8_unchecked(facet.encoded_bytes())
                })
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], "/");
    }
}
