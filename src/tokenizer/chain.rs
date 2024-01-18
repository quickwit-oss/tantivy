use tokenizer_api::{Token, TokenStream, Tokenizer};

/// Trait to allow easily chaining two [`Tokenizer`].
pub trait ChainTokenizerExt: Sized {
    /// Produce a [`Tokenizer`] which runs through the first tokenizer, and then through the second.
    fn chain<T: Tokenizer>(self, next: T) -> ChainTokenizer<Self, T>;
}

impl<F: Tokenizer> ChainTokenizerExt for F {
    fn chain<T: Tokenizer>(self, second: T) -> ChainTokenizer<Self, T> {
        ChainTokenizer {
            first: self,
            second,
        }
    }
}

/// A tokenizer running through the first tokenizer and then through the second.
#[derive(Clone)]
pub struct ChainTokenizer<F, S> {
    first: F,
    second: S,
}

impl<F, S> ChainTokenizer<F, S>
where
    F: Tokenizer,
    S: Tokenizer,
{
    /// Create a new tokenzier, chaining the two provided ones.
    pub fn new(first: F, second: S) -> Self {
        Self { first, second }
    }
}

impl<F, S> Tokenizer for ChainTokenizer<F, S>
where
    F: Tokenizer,
    S: Tokenizer,
{
    type TokenStream<'a> = ChainTokenStream<'a, F, S>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ChainTokenStream {
            state: State::First,
            first: self.first.token_stream(text),
            second: self.second.token_stream(text),
            dummy: Token::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    First,
    Second,
    Done,
}

pub struct ChainTokenStream<'a, F, S>
where
    F: Tokenizer,
    S: Tokenizer,
{
    first: F::TokenStream<'a>,
    second: S::TokenStream<'a>,
    state: State,
    dummy: Token,
}

impl<'a, F, S> TokenStream for ChainTokenStream<'a, F, S>
where
    F: Tokenizer,
    S: Tokenizer,
{
    fn advance(&mut self) -> bool {
        match self.state {
            State::First => {
                if self.first.advance() {
                    true
                } else {
                    self.state = State::Second;
                    self.advance()
                }
            }
            State::Second => {
                if self.second.advance() {
                    true
                } else {
                    self.state = State::Done;
                    false
                }
            }
            State::Done => false,
        }
    }

    fn token(&self) -> &Token {
        match self.state {
            State::First => self.first.token(),
            State::Second => self.second.token(),
            State::Done => &self.dummy,
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        match self.state {
            State::First => self.first.token_mut(),
            State::Second => self.second.token_mut(),
            State::Done => &mut self.dummy,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokenizer_api::TokenFilter;

    use super::*;
    use crate::tokenizer::empty_tokenizer::EmptyTokenizer;
    use crate::tokenizer::{LowerCaser, RawTokenizer, SimpleTokenizer};

    fn assert_chain<'a>(
        first: impl Tokenizer,
        second: impl Tokenizer,
        input: &str,
        expected: impl IntoIterator<Item = &'a str>,
    ) {
        let mut chain = ChainTokenizer::new(first, second);
        let mut stream = chain.token_stream(input);
        let mut result = vec![];
        while let Some(token) = stream.next() {
            result.push(token.text.to_string());
        }
        let expected = expected.into_iter().collect::<Vec<_>>();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_empty() {
        assert_chain(EmptyTokenizer, EmptyTokenizer, "", []);
    }

    #[test]
    fn test_simple() {
        assert_chain(
            SimpleTokenizer::default(),
            LowerCaser.transform(SimpleTokenizer::default()),
            "Foo Bar Baz",
            ["Foo", "Bar", "Baz", "foo", "bar", "baz"],
        );
    }

    #[test]
    fn test_empty_simple() {
        assert_chain(
            EmptyTokenizer,
            SimpleTokenizer::default(),
            "Foo Bar Baz",
            ["Foo", "Bar", "Baz"],
        );
    }

    #[test]
    fn test_simple_empty() {
        assert_chain(
            SimpleTokenizer::default(),
            EmptyTokenizer,
            "Foo Bar Baz",
            ["Foo", "Bar", "Baz"],
        );
    }

    #[test]
    fn test_chain_twice() {
        assert_chain(
            SimpleTokenizer::default(),
            LowerCaser
                .transform(SimpleTokenizer::default())
                .chain(RawTokenizer::default()),
            "FOO BAR BAZ",
            ["FOO", "BAR", "BAZ", "foo", "bar", "baz", "FOO BAR BAZ"],
        );
    }
}
