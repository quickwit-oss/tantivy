use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};

use super::{Token, TokenFilter, TokenStream, Tokenizer};

/// A [`TokenFilter`] which splits compound words into their parts
/// based on a given dictionary.
///
/// Words only will be split if they can be fully decomposed into
/// consecutive matches into the given dictionary.
///
/// This is mostly useful to split [compound nouns][compound] common to many
/// Germanic languages into their constituents.
///
/// # Example
///
/// The quality of the dictionary determines the quality of the splits,
/// e.g. the missing stem "back" of "backen" implies that "brotbackautomat"
/// is not split in the following example.
///
/// ```rust
/// use tantivy::tokenizer::{SimpleTokenizer, SplitCompoundWords, TextAnalyzer};
///
/// let mut tokenizer =
///        TextAnalyzer::builder(SimpleTokenizer::default())
///        .filter(
///            SplitCompoundWords::from_dictionary([
///                 "dampf", "schiff", "fahrt", "brot", "backen", "automat",
///            ])
///            .unwrap()
///        )
///        .build();
/// {
///     let mut stream = tokenizer.token_stream("dampfschifffahrt");
///     assert_eq!(stream.next().unwrap().text, "dampf");
///     assert_eq!(stream.next().unwrap().text, "schiff");
///     assert_eq!(stream.next().unwrap().text, "fahrt");
///     assert_eq!(stream.next(), None);
/// }
/// let mut stream = tokenizer.token_stream("brotbackautomat");
/// assert_eq!(stream.next().unwrap().text, "brotbackautomat");
/// assert_eq!(stream.next(), None);
/// ```
///
/// [compound]: https://en.wikipedia.org/wiki/Compound_(linguistics)
#[derive(Clone)]
pub struct SplitCompoundWords {
    dict: AhoCorasick,
}

impl SplitCompoundWords {
    /// Create a filter from a given dictionary.
    ///
    /// The dictionary will be used to construct an [`AhoCorasick`] automaton
    /// with reasonable defaults. See [`from_automaton`][Self::from_automaton] if
    /// more control over its construction is required.
    pub fn from_dictionary<I, P>(dict: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        let dict = AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(dict)
            .map_err(|err| {
                crate::TantivyError::InvalidArgument(format!(
                    "Failed to build Aho-Corasick automaton from dictionary: {err}"
                ))
            })?;

        Ok(Self::from_automaton(dict))
    }

    /// Create a filter from a given automaton.
    ///
    /// The automaton should use one of the leftmost-first match kinds
    /// and it should not be anchored.
    pub fn from_automaton(dict: AhoCorasick) -> Self {
        Self { dict }
    }
}

impl TokenFilter for SplitCompoundWords {
    type Tokenizer<T: Tokenizer> = SplitCompoundWordsFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> SplitCompoundWordsFilter<T> {
        SplitCompoundWordsFilter {
            dict: self.dict,
            inner: tokenizer,
            cuts: Vec::new(),
            part: Token::default(),
        }
    }
}

#[derive(Clone)]
pub struct SplitCompoundWordsFilter<T> {
    dict: AhoCorasick,
    inner: T,
    cuts: Vec<usize>,
    part: Token,
}

impl<T: Tokenizer> Tokenizer for SplitCompoundWordsFilter<T> {
    type TokenStream<'a> = SplitCompoundWordsTokenStream<'a, T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.cuts.clear();
        SplitCompoundWordsTokenStream {
            dict: self.dict.clone(),
            tail: self.inner.token_stream(text),
            cuts: &mut self.cuts,
            part: &mut self.part,
            next_part: None,
        }
    }
}

pub struct SplitCompoundWordsTokenStream<'a, T> {
    dict: AhoCorasick,
    tail: T,
    cuts: &'a mut Vec<usize>,
    // Holds the part currently yielded, reusing its text buffer
    // from one part to the next.
    part: &'a mut Token,
    // Index in `self.cuts` of the part held by `self.part`,
    // or `None` if `self.tail.token()` is yielded as is.
    next_part: Option<usize>,
}

impl<T: TokenStream> SplitCompoundWordsTokenStream<'_, T> {
    // Will fill `self.cuts` with the start of each part if `self.tail.token()`
    // can fully be split into consecutive matches against `self.dict`.
    fn split(&mut self) {
        let token = self.tail.token();
        let text = token.text.as_str();

        self.cuts.clear();
        let mut pos = 0;

        for match_ in self.dict.find_iter(text) {
            if pos != match_.start() {
                break;
            }

            self.cuts.push(pos);
            pos = match_.end();
        }

        if pos == token.text.len() && !self.cuts.is_empty() {
            self.set_part(0);
        }
    }

    // Copies the part at `index` in `self.cuts` into `self.part`.
    fn set_part(&mut self, index: usize) {
        let token = self.tail.token();
        let start = self.cuts[index];
        let end = self
            .cuts
            .get(index + 1)
            .copied()
            .unwrap_or(token.text.len());
        self.part.text.clear();
        self.part.text.push_str(&token.text[start..end]);
        self.part.offset_from = token.offset_from;
        self.part.offset_to = token.offset_to;
        self.part.position = token.position;
        self.part.position_length = token.position_length;
        self.next_part = Some(index);
    }
}

impl<T: TokenStream> TokenStream for SplitCompoundWordsTokenStream<'_, T> {
    fn advance(&mut self) -> bool {
        if let Some(index) = self.next_part.take() {
            if index + 1 < self.cuts.len() {
                self.set_part(index + 1);
                return true;
            }
        }

        if !self.tail.advance() {
            return false;
        }

        // Will yield either `self.part` or
        // `self.tail.token()` if it could not be split.
        self.split();
        true
    }

    fn token(&self) -> &Token {
        if self.next_part.is_some() {
            self.part
        } else {
            self.tail.token()
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        if self.next_part.is_some() {
            self.part
        } else {
            self.tail.token_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::{SimpleTokenizer, TextAnalyzer};

    #[test]
    fn splitting_compound_words_works() {
        let mut tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(SplitCompoundWords::from_dictionary(["foo", "bar"]).unwrap())
            .build();

        {
            let mut stream = tokenizer.token_stream("");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foo bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobarbaz");
            assert_eq!(stream.next().unwrap().text, "foobarbaz");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("baz foobar qux");
            assert_eq!(stream.next().unwrap().text, "baz");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "qux");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar foo bar foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobazbar foo bar foobar");
            assert_eq!(stream.next().unwrap().text, "foobazbar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("foobar qux foobar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "qux");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next(), None);
        }

        {
            let mut stream = tokenizer.token_stream("barfoo");
            assert_eq!(stream.next().unwrap().text, "bar");
            assert_eq!(stream.next().unwrap().text, "foo");
            assert_eq!(stream.next(), None);
        }
    }
}
