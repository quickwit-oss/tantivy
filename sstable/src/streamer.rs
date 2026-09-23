use std::cmp::Ordering;
use std::io;
use std::ops::Bound;

use tantivy_fst::Automaton;
use tantivy_fst::automaton::AlwaysMatch;

use crate::delta::DeltaKeyComparator;
use crate::dictionary::Dictionary;
use crate::{DeltaReader, SSTable, TermOrdinal};

/// `StreamerBuilder` is a helper object used to define
/// a range of terms that should be streamed.
pub struct StreamerBuilder<'a, TSSTable, A = AlwaysMatch>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
    term_dict: &'a Dictionary<TSSTable>,
    automaton: A,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    limit: Option<u64>,
}

fn bound_as_byte_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound.as_ref() {
        Bound::Included(key) => Bound::Included(key.as_slice()),
        Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[inline(always)]
fn matches_upper_bound(
    comparator: &mut DeltaKeyComparator,
    upper_bound: &Bound<Vec<u8>>,
    common_prefix_len: usize,
    suffix: &[u8],
) -> bool {
    let (upper_bound_key, inclusive) = match upper_bound {
        Bound::Unbounded => return true,
        Bound::Included(upper_bound_key) => (upper_bound_key, true),
        Bound::Excluded(upper_bound_key) => (upper_bound_key, false),
    };
    let ordering = comparator.compare_across_blocks(upper_bound_key, common_prefix_len, suffix);
    ordering == Ordering::Less || inclusive && ordering == Ordering::Equal
}

impl<'a, TSSTable, A> StreamerBuilder<'a, TSSTable, A>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
    pub(crate) fn new(term_dict: &'a Dictionary<TSSTable>, automaton: A) -> Self {
        StreamerBuilder {
            term_dict,
            automaton,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            limit: None,
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

    /// Load no more data than what's required to to get `limit`
    /// matching entries.
    ///
    /// The resulting [`Streamer`] can still return marginally
    /// more than `limit` elements.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    fn delta_reader(&self) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let key_range = (
            bound_as_byte_slice(&self.lower),
            bound_as_byte_slice(&self.upper),
        );
        self.term_dict
            .sstable_delta_reader_for_key_range(key_range, self.limit, &self.automaton)
    }

    async fn delta_reader_async(
        &self,
        merge_holes_under_bytes: usize,
    ) -> io::Result<DeltaReader<TSSTable::ValueReader>> {
        let key_range = (
            bound_as_byte_slice(&self.lower),
            bound_as_byte_slice(&self.upper),
        );
        self.term_dict
            .sstable_delta_reader_for_key_range_async(
                key_range,
                self.limit,
                &self.automaton,
                merge_holes_under_bytes,
            )
            .await
    }

    fn into_stream_given_delta_reader(
        self,
        delta_reader: DeltaReader<<TSSTable as SSTable>::ValueReader>,
    ) -> io::Result<Streamer<'a, TSSTable, A>> {
        let start_state = self.automaton.start();
        let start_key = bound_as_byte_slice(&self.lower);

        let first_term = match start_key {
            Bound::Included(key) | Bound::Excluded(key) => self
                .term_dict
                .sstable_index
                .get_block_with_key(key)
                .map(|block| block.first_ordinal)
                .unwrap_or(0),
            Bound::Unbounded => 0,
        };

        Ok(Streamer {
            automaton: self.automaton,
            states: vec![start_state],
            delta_reader,
            key: Vec::new(),
            term_ord: first_term.checked_sub(1),
            lower_bound_reached: self.lower == Bound::Unbounded,
            lower_bound: self.lower,
            upper_bound: self.upper,
            upper_bound_comparator: DeltaKeyComparator::new(),
            _lifetime: std::marker::PhantomData,
        })
    }

    /// See `into_stream(..)`
    pub async fn into_stream_async(self) -> io::Result<Streamer<'a, TSSTable, A>> {
        self.into_stream_async_merging_holes(0).await
    }

    /// Same as `into_stream_async`, but tries to issue a single io operation when requesting
    /// blocks that are not consecutive, but also less than `merge_holes_under_bytes` bytes apart.
    pub async fn into_stream_async_merging_holes(
        self,
        merge_holes_under_bytes: usize,
    ) -> io::Result<Streamer<'a, TSSTable, A>> {
        let delta_reader = self.delta_reader_async(merge_holes_under_bytes).await?;
        self.into_stream_given_delta_reader(delta_reader)
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `StreamerBuilder`.
    pub fn into_stream(self) -> io::Result<Streamer<'a, TSSTable, A>> {
        let delta_reader = self.delta_reader()?;
        self.into_stream_given_delta_reader(delta_reader)
    }
}

/// `Streamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct Streamer<'a, TSSTable, A = AlwaysMatch>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
    automaton: A,
    states: Vec<A::State>,
    delta_reader: crate::DeltaReader<TSSTable::ValueReader>,
    key: Vec<u8>,
    term_ord: Option<TermOrdinal>,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    upper_bound_comparator: DeltaKeyComparator,
    // this field is used to please the type-interface of a dictionary in tantivy
    _lifetime: std::marker::PhantomData<&'a ()>,
    lower_bound_reached: bool,
}

impl<TSSTable> Streamer<'_, TSSTable, AlwaysMatch>
where TSSTable: SSTable
{
    pub fn empty() -> Self {
        Streamer {
            automaton: AlwaysMatch,
            states: vec![AlwaysMatch.start()],
            delta_reader: DeltaReader::empty(),
            key: Vec::new(),
            term_ord: None,
            lower_bound_reached: true,
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Unbounded,
            upper_bound_comparator: DeltaKeyComparator::new(),
            _lifetime: std::marker::PhantomData,
        }
    }
}

impl<TSSTable, A> Streamer<'_, TSSTable, A>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
    #[inline(always)]
    fn advance_delta_reader(&mut self) -> bool {
        if !self.delta_reader.advance().unwrap() {
            return false;
        }
        // An automaton prunes whole blocks, so the ordinal is not simply the previous one
        // plus one: on entering a new slice it jumps to that slice's first term ordinal.
        // Counting alone would report a term's position among the blocks actually scanned.
        self.term_ord = Some(match self.delta_reader.take_first_ordinal() {
            Some(first_ordinal) => first_ordinal,
            None => self
                .term_ord
                .map(|term_ord| term_ord + 1u64)
                .unwrap_or(0u64),
        });
        true
    }

    /// Make progress up to the lower bound
    ///
    /// Returns whether the reader was positioned on a key matching the lower bound.
    /// If false, the delta_reader has been exhausted without finding such a key.
    fn initialize(&mut self) -> bool {
        debug_assert!(!self.lower_bound_reached);
        let mut lower_bound_comparator = DeltaKeyComparator::new();
        while self.advance_delta_reader() {
            let common_prefix_len = self.delta_reader.common_prefix_len();
            let suffix = self.delta_reader.suffix();
            let (lower_bound_key, inclusive) = match &self.lower_bound {
                Bound::Unbounded => unreachable!("unbounded streamers do not need initialization"),
                Bound::Included(lower_bound_key) => (lower_bound_key, true),
                Bound::Excluded(lower_bound_key) => (lower_bound_key, false),
            };
            let ordering = lower_bound_comparator.compare_across_blocks(
                lower_bound_key,
                common_prefix_len,
                suffix,
            );
            let match_lower_bound =
                ordering == Ordering::Greater || inclusive && ordering == Ordering::Equal;
            if match_lower_bound {
                self.key.clear();
                self.key
                    .extend_from_slice(&lower_bound_key[..common_prefix_len]);
                self.key.extend_from_slice(suffix);
                let mut state: A::State = self.states.last().unwrap().clone();
                for b in &self.key {
                    state = self.automaton.accept(&state, *b);
                    self.states.push(state.clone());
                }
                self.lower_bound_reached = true;
                return true;
            }
        }
        self.lower_bound_reached = true;
        false
    }

    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an uninitialized state.
    pub fn advance(&mut self) -> bool {
        if !self.lower_bound_reached {
            if !self.initialize() {
                // no key higher than lower-bound at all
                return false;
            }
            if !matches_upper_bound(
                &mut self.upper_bound_comparator,
                &self.upper_bound,
                0,
                &self.key,
            ) {
                return false;
            }
            if self.automaton.is_match(self.states.last().unwrap()) {
                return true;
            }
        }

        // (always_match, no_bound)
        match (
            self.automaton
                .will_always_match(&self.states.first().unwrap()),
            self.upper_bound == Bound::Unbounded,
        ) {
            (true, true) => self.advance_inner::<true, true>(),
            (true, false) => self.advance_inner::<true, false>(),
            (false, true) => self.advance_inner::<false, true>(),
            (false, false) => self.advance_inner::<false, false>(),
        }
    }

    fn advance_inner<const AUTOMATON_MATCH: bool, const NO_BOUND: bool>(&mut self) -> bool {
        while self.advance_delta_reader() {
            let common_prefix_len = self.delta_reader.common_prefix_len();
            self.states.truncate(common_prefix_len + 1);

            // TODO we could detect when we reach an always match state, and no longer check state
            // until we truncate enough (e.g. for the regex `my_prefix.*`, or `.*my_infix.*`)
            // TODO we could detect when we reach a !can_match, and skip both state and key
            // computation until we truncate that can_t_match out of our state
            let mut state: A::State = self.states.last().unwrap().clone();
            if !AUTOMATON_MATCH {
                for &b in self.delta_reader.suffix() {
                    state = self.automaton.accept(&state, b);
                    self.states.push(state.clone());
                }
            }

            self.key.truncate(common_prefix_len);
            self.key.extend_from_slice(self.delta_reader.suffix());

            // TODO there is an idea where we only look at the upper bound when our delta_reader
            // reached the last block (if we pruned blocks beforehand (do we always?) we cannot
            // find that key before that block)
            if !NO_BOUND
                && !matches_upper_bound(
                    &mut self.upper_bound_comparator,
                    &self.upper_bound,
                    common_prefix_len,
                    self.delta_reader.suffix(),
                )
            {
                return false;
            }
            if AUTOMATON_MATCH || self.automaton.is_match(&state) {
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
    pub fn value(&self) -> &TSSTable::Value {
        self.delta_reader.value()
    }

    /// Return the next `(key, value)` pair.
    #[expect(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(&[u8], &TSSTable::Value)> {
        if self.advance() {
            Some((self.key(), self.value()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use common::OwnedBytes;

    use crate::{Dictionary, MonotonicU64SSTable};

    fn create_test_dictionary() -> io::Result<Dictionary<MonotonicU64SSTable>> {
        let mut dict_builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new())?;
        dict_builder.insert(b"abaisance", &0)?;
        dict_builder.insert(b"abalation", &1)?;
        dict_builder.insert(b"abalienate", &2)?;
        dict_builder.insert(b"abandon", &3)?;
        let buffer = dict_builder.finish()?;
        let owned_bytes = OwnedBytes::new(buffer);
        Dictionary::from_bytes(owned_bytes)
    }

    #[test]
    fn test_sstable_stream() -> io::Result<()> {
        let dict = create_test_dictionary()?;
        let mut streamer = dict.stream()?;
        assert!(streamer.advance());
        assert_eq!(streamer.key(), b"abaisance");
        assert_eq!(streamer.value(), &0);
        assert!(streamer.advance());
        assert_eq!(streamer.key(), b"abalation");
        assert_eq!(streamer.value(), &1);
        assert!(streamer.advance());
        assert_eq!(streamer.key(), b"abalienate");
        assert_eq!(streamer.value(), &2);
        assert!(streamer.advance());
        assert_eq!(streamer.key(), b"abandon");
        assert_eq!(streamer.value(), &3);
        assert!(!streamer.advance());
        Ok(())
    }

    #[test]
    fn test_sstable_search() -> io::Result<()> {
        let term_dict = create_test_dictionary()?;
        let ptn = tantivy_fst::Regex::new("ab.*t.*").unwrap();
        let mut term_streamer = term_dict.search(ptn).into_stream()?;
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalation");
        assert_eq!(term_streamer.value(), &1u64);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.key(), b"abalienate");
        assert_eq!(term_streamer.value(), &2u64);
        assert!(!term_streamer.advance());
        Ok(())
    }

    // TODO add test for sparse search with a block of poison (starts with 0xffffffff) => such a
    // block instantly causes an unexpected EOF error
}
