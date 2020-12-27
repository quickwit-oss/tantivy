use super::{Token, TokenStream, Tokenizer};
use crate::schema::FACET_SEP_BYTE;

/// The `FacetTokenizer` process a `Facet` binary representation
/// and emits a token for all of its parent.
///
/// For instance,  `/america/north_america/canada`
/// will emit the three following tokens
///     - `/america/north_america/canada`
///     - `/america/north_america`
///     - `/america`
#[derive(Clone, Debug)]
pub struct FacetTokenizer;

#[derive(Clone, Debug)]
enum State {
    RootFacetNotEmitted,
    UpToPosition(usize), //< we already emitted facet prefix up to &text[..cursor]
    Terminated,
}

#[derive(Clone, Debug)]
pub struct FacetTokenStream<'a> {
    text: &'a str,
    state: State,
    token: Token,
}

impl<'a> Tokenizer<'a> for FacetTokenizer {
    type Iter = FacetTokenStream<'a>;
    fn token_stream(&self, text: &'a str) -> Self::Iter {
        FacetTokenStream {
            text,
            state: State::RootFacetNotEmitted, //< pos is the first char that has not been processed yet.
            token: Token::default(),
        }
    }
}

impl<'a> Iterator for FacetTokenStream<'a> {
    type Item = Token;
    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            State::RootFacetNotEmitted => {
                self.state = if self.text.is_empty() {
                    State::Terminated
                } else {
                    State::UpToPosition(0)
                };
            }
            State::UpToPosition(cursor) => {
                if let Some(next_sep_pos) = self.text.as_bytes()[cursor + 1..]
                    .iter()
                    .position(|&b| b == FACET_SEP_BYTE)
                    .map(|pos| cursor + 1 + pos)
                {
                    let facet_part = &self.text[cursor..next_sep_pos];
                    self.token.text.push_str(facet_part);
                    self.state = State::UpToPosition(next_sep_pos);
                } else {
                    let facet_part = &self.text[cursor..];
                    self.token.text.push_str(facet_part);
                    self.state = State::Terminated;
                }
            }
            State::Terminated => return None,
        };
        Some(self.token.clone())
    }
}

impl<'a> TokenStream for FacetTokenStream<'a> {}

#[cfg(test)]
mod tests {

    use super::FacetTokenizer;
    use crate::schema::Facet;
    use crate::tokenizer::{Token, Tokenizer};

    #[test]
    fn test_facet_tokenizer() {
        let facet = Facet::from_path(vec!["top", "a", "b"]);
        let mut tokens = vec![];
        {
            let mut add_token = |token: &Token| {
                let facet = Facet::from_encoded(token.text.as_bytes().to_owned()).unwrap();
                tokens.push(format!("{}", facet));
            };
            FacetTokenizer
                .token_stream(facet.encoded_str())
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
                let facet = Facet::from_encoded(token.text.as_bytes().to_owned()).unwrap(); // ok test
                tokens.push(format!("{}", facet));
            };
            FacetTokenizer
                .token_stream(facet.encoded_str()) // ok test
                .process(&mut add_token);
        }
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], "/");
    }
}
