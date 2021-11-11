use std::io;
use std::ops::Bound;

use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::Automaton;

use super::TermDictionary;
use crate::postings::TermInfo;
use crate::termdict::sstable_termdict::TermInfoReader;
use crate::termdict::TermOrdinal;

/// `TermStreamerBuilder` is a helper object used to define
/// a range of terms that should be streamed.
pub struct TermStreamerBuilder<'a, A = AlwaysMatch>
where
    A: Automaton,
    A::State: Clone,
{
    term_dict: &'a TermDictionary,
    automaton: A,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl<'a, A> TermStreamerBuilder<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    pub(crate) fn new(term_dict: &'a TermDictionary, automaton: A) -> Self {
        TermStreamerBuilder {
            term_dict,
            automaton,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }

    /// Limit the range to terms greater or equal to the bound
    pub fn ge<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.lower = Bound::Included(bound.as_ref().to_owned());
        self
    }

    /// Limit the range to terms strictly greater than the bound
    pub fn gt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.lower = Bound::Excluded(bound.as_ref().to_owned());
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn le<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.upper = Bound::Included(bound.as_ref().to_owned());
        self
    }

    /// Limit the range to terms lesser or equal to the bound
    pub fn lt<T: AsRef<[u8]>>(mut self, bound: T) -> Self {
        self.upper = Bound::Excluded(bound.as_ref().to_owned());
        self
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `TermStreamerBuilder`.
    pub fn into_stream(self) -> io::Result<TermStreamer<'a, A>> {
        // TODO Optimize by skipping to the right first block.
        let start_state = self.automaton.start();
        let delta_reader = self.term_dict.sstable_delta_reader()?;
        Ok(TermStreamer {
            automaton: self.automaton,
            states: vec![start_state],
            delta_reader,
            key: Vec::new(),
            term_ord: None,
            lower_bound: self.lower,
            upper_bound: self.upper,
        })
    }
}

/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct TermStreamer<'a, A = AlwaysMatch>
where
    A: Automaton,
    A::State: Clone,
{
    automaton: A,
    states: Vec<A::State>,
    delta_reader: super::sstable::DeltaReader<'a, TermInfoReader>,
    key: Vec<u8>,
    term_ord: Option<TermOrdinal>,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
}

impl<'a, A> TermStreamer<'a, A>
where
    A: Automaton,
    A::State: Clone,
{
    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an unitialized state.
    pub fn advance(&mut self) -> bool {
        while self.delta_reader.advance().unwrap() {
            self.term_ord = Some(
                self.term_ord
                    .map(|term_ord| term_ord + 1u64)
                    .unwrap_or(0u64),
            );
            let common_prefix_len = self.delta_reader.common_prefix_len();
            self.states.truncate(common_prefix_len + 1);
            self.key.truncate(common_prefix_len);
            let mut state: A::State = self.states.last().unwrap().clone();
            for &b in self.delta_reader.suffix() {
                state = self.automaton.accept(&state, b);
                self.states.push(state.clone());
            }
            self.key.extend_from_slice(self.delta_reader.suffix());
            let match_lower_bound = match &self.lower_bound {
                Bound::Unbounded => true,
                Bound::Included(lower_bound_key) => lower_bound_key[..] <= self.key[..],
                Bound::Excluded(lower_bound_key) => lower_bound_key[..] < self.key[..],
            };
            if !match_lower_bound {
                continue;
            }
            // We match the lower key once. All subsequent keys will pass that bar.
            self.lower_bound = Bound::Unbounded;
            let match_upper_bound = match &self.upper_bound {
                Bound::Unbounded => true,
                Bound::Included(upper_bound_key) => upper_bound_key[..] >= self.key[..],
                Bound::Excluded(upper_bound_key) => upper_bound_key[..] > self.key[..],
            };
            if !match_upper_bound {
                return false;
            }
            if self.automaton.is_match(&state) {
                return true;
            }
        }
        false
    }

    /// Returns the `TermOrdinal` of the given term.
    ///
    /// May panic if the called as `.advance()` as never
    /// been called before.
    pub fn term_ord(&self) -> TermOrdinal {
        self.term_ord.unwrap_or(0u64)
    }

    /// Accesses the current key.
    ///
    /// `.key()` should return the key that was returned
    /// by the `.next()` method.
    ///
    /// If the end of the stream as been reached, and `.next()`
    /// has been called and returned `None`, `.key()` remains
    /// the value of the last key encountered.
    ///
    /// Before any call to `.next()`, `.key()` returns an empty array.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Accesses the current value.
    ///
    /// Calling `.value()` after the end of the stream will return the
    /// last `.value()` encountered.
    ///
    /// # Panics
    ///
    /// Calling `.value()` before the first call to `.advance()` returns
    /// `V::default()`.
    pub fn value(&self) -> &TermInfo {
        self.delta_reader.value()
    }

    /// Return the next `(key, value)` pair.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::should_implement_trait))]
    pub fn next(&mut self) -> Option<(&[u8], &TermInfo)> {
        if self.advance() {
            Some((self.key(), self.value()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::TermDictionary;
    use crate::directory::OwnedBytes;
    use crate::postings::TermInfo;

    fn make_term_info(i: usize) -> TermInfo {
        TermInfo {
            doc_freq: 1000u32 + i as u32,
            postings_range: (i + 10) * (i * 10)..((i + 1) + 10) * ((i + 1) * 10),
            positions_range: i * 500..(i + 1) * 500,
        }
    }

    fn create_test_term_dictionary() -> crate::Result<TermDictionary> {
        let mut term_dict_builder = super::super::TermDictionaryBuilder::create(Vec::new())?;
        term_dict_builder.insert(b"abaisance", &make_term_info(0))?;
        term_dict_builder.insert(b"abalation", &make_term_info(1))?;
        term_dict_builder.insert(b"abalienate", &make_term_info(2))?;
        term_dict_builder.insert(b"abandon", &make_term_info(3))?;
        let buffer = term_dict_builder.finish()?;
        let owned_bytes = OwnedBytes::new(buffer);
        TermDictionary::from_bytes(owned_bytes)
    }

    #[test]
    fn test_sstable_stream() -> crate::Result<()> {
        let term_dict = create_test_term_dictionary()?;
        let mut term_streamer = term_dict.stream()?;
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abaisance");
        assert_eq!(term_streamer.value().doc_freq, 1000u32);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalation");
        assert_eq!(term_streamer.value().doc_freq, 1001u32);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalienate");
        assert_eq!(term_streamer.value().doc_freq, 1002u32);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abandon");
        assert_eq!(term_streamer.value().doc_freq, 1003u32);
        assert!(!term_streamer.advance());
        Ok(())
    }

    #[test]
    fn test_sstable_search() -> crate::Result<()> {
        let term_dict = create_test_term_dictionary()?;
        let ptn = tantivy_fst::Regex::new("ab.*t.*").unwrap();
        let mut term_streamer = term_dict.search(ptn).into_stream()?;
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalation");
        assert_eq!(term_streamer.value().doc_freq, 1001u32);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalienate");
        assert_eq!(term_streamer.value().doc_freq, 1002u32);
        assert!(!term_streamer.advance());
        Ok(())
    }
}
