use std::cmp::Ordering;
use std::io;
use std::marker::PhantomData;
use std::ops::Bound;

use common::{KeyTracking, WithoutKeys};
use tantivy_fst::Automaton;
use tantivy_fst::automaton::AlwaysMatch;

use crate::delta::DeltaKeyComparator;
use crate::dictionary::Dictionary;
use crate::{DeltaReader, SSTable, TermOrdinal};

/// `StreamerBuilder` is a helper object used to define
/// a range of terms that should be streamed.
pub struct StreamerBuilder<'a, TSSTable, A = AlwaysMatch, K = Vec<u8>>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
    K: KeyTracking,
{
    term_dict: &'a Dictionary<TSSTable>,
    automaton: A,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    limit: Option<u64>,
    _key_tracking: PhantomData<K>,
}

fn bound_as_byte_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound.as_ref() {
        Bound::Included(key) => Bound::Included(key.as_slice()),
        Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Same as `matches_upper_bound`, for a key given as `prefix + suffix` rather than as a delta.
fn prefix_and_suffix_match_upper_bound(
    comparator: &mut DeltaKeyComparator,
    upper_bound: &Bound<Vec<u8>>,
    prefix: &[u8],
    suffix: &[u8],
) -> bool {
    let (upper_bound_key, inclusive) = match upper_bound {
        Bound::Unbounded => return true,
        Bound::Included(upper_bound_key) => (upper_bound_key, true),
        Bound::Excluded(upper_bound_key) => (upper_bound_key, false),
    };
    let ordering = comparator.compare_prefix_and_suffix(upper_bound_key, prefix, suffix);
    ordering == Ordering::Less || inclusive && ordering == Ordering::Equal
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

impl<'a, TSSTable, A> StreamerBuilder<'a, TSSTable, A, Vec<u8>>
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
            _key_tracking: PhantomData,
        }
    }

    /// Makes the resulting [`Streamer`] skip rebuilding keys (as an optimisation).
    pub fn without_keys(self) -> StreamerBuilder<'a, TSSTable, A, WithoutKeys> {
        StreamerBuilder {
            term_dict: self.term_dict,
            automaton: self.automaton,
            lower: self.lower,
            upper: self.upper,
            limit: self.limit,
            _key_tracking: PhantomData,
        }
    }
}

impl<'a, TSSTable, A, K> StreamerBuilder<'a, TSSTable, A, K>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
    K: KeyTracking,
{
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
    ) -> io::Result<Streamer<'a, TSSTable, A, K>> {
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

        let always_match_at = if self.automaton.will_always_match(&start_state) {
            Some(0)
        } else {
            None
        };

        Ok(Streamer {
            automaton: self.automaton,
            states: vec![start_state],
            always_match_at,
            delta_reader,
            key: K::make_default(),
            term_ord: first_term.checked_sub(1),
            lower_bound_reached: self.lower == Bound::Unbounded,
            lower_bound: self.lower,
            upper_bound: self.upper,
            upper_bound_comparator: DeltaKeyComparator::new(),
            _lifetime: std::marker::PhantomData,
        })
    }

    /// See `into_stream(..)`
    pub async fn into_stream_async(self) -> io::Result<Streamer<'a, TSSTable, A, K>> {
        self.into_stream_async_merging_holes(0).await
    }

    /// Same as `into_stream_async`, but tries to issue a single io operation when requesting
    /// blocks that are not consecutive, but also less than `merge_holes_under_bytes` bytes apart.
    pub async fn into_stream_async_merging_holes(
        self,
        merge_holes_under_bytes: usize,
    ) -> io::Result<Streamer<'a, TSSTable, A, K>> {
        let delta_reader = self.delta_reader_async(merge_holes_under_bytes).await?;
        self.into_stream_given_delta_reader(delta_reader)
    }

    /// Creates the stream corresponding to the range
    /// of terms defined using the `StreamerBuilder`.
    pub fn into_stream(self) -> io::Result<Streamer<'a, TSSTable, A, K>> {
        let delta_reader = self.delta_reader()?;
        self.into_stream_given_delta_reader(delta_reader)
    }
}

/// `Streamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub struct Streamer<'a, TSSTable, A = AlwaysMatch, K = Vec<u8>>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
    K: KeyTracking,
{
    automaton: A,
    states: Vec<A::State>,
    delta_reader: crate::DeltaReader<TSSTable::ValueReader>,
    key: K,
    term_ord: Option<TermOrdinal>,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    upper_bound_comparator: DeltaKeyComparator,
    // this field is used to please the type-interface of a dictionary in tantivy
    _lifetime: std::marker::PhantomData<&'a ()>,
    lower_bound_reached: bool,
    always_match_at: Option<usize>,
}

impl<TSSTable> Streamer<'_, TSSTable, AlwaysMatch>
where TSSTable: SSTable
{
    pub fn empty() -> Self {
        Streamer {
            automaton: AlwaysMatch,
            states: vec![AlwaysMatch.start()],
            always_match_at: Some(0),
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

impl<TSSTable, A, K> Streamer<'_, TSSTable, A, K>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
    K: KeyTracking,
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
    /// Returns whether the reader was positioned on a key within both the lower and the upper
    /// bound. If false, there is no such key: either the delta_reader has been exhausted without
    /// reaching the lower bound, or the first key past the lower bound exceeds the upper bound.
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
                // The previous key is unknown, but the comparator guarantees it starts with
                // `implied_prefix`. Replaying `implied_prefix` as the previous key turns the
                // current entry into a regular delta.
                let implied_prefix = &lower_bound_key[..common_prefix_len];
                self.key.set_key(implied_prefix);
                self.key.update_with_prefix(common_prefix_len, suffix);
                let mut state: A::State = self.states.last().unwrap().clone();
                for b in implied_prefix.iter().copied().chain(suffix.iter().copied()) {
                    state = self.automaton.accept(&state, b);
                    self.states.push(state.clone());
                }
                self.lower_bound_reached = true;
                return prefix_and_suffix_match_upper_bound(
                    &mut self.upper_bound_comparator,
                    &self.upper_bound,
                    implied_prefix,
                    suffix,
                );
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
                // no key within the bounds at all
                return false;
            }
            if self.automaton.is_match(self.states.last().unwrap()) {
                return true;
            }
        }

        match (
            // we could check always_match_at == Some(0), but this actually gets
            // inlined into `true` with AlwaysMatch, which is even faster
            self.automaton
                .will_always_match(&self.states.first().unwrap()),
            self.upper_bound == Bound::Unbounded,
        ) {
            (true, true) => self.advance_always_match::<true>(),
            (true, false) => self.advance_always_match::<false>(),
            (false, true) => self.advance_with_automaton::<true>(),
            (false, false) => self.advance_with_automaton::<false>(),
        }
    }

    fn advance_always_match<const NO_BOUND: bool>(&mut self) -> bool {
        if !self.advance_delta_reader() {
            return false;
        }
        self.reconstruct_key_and_check_upper_bound::<NO_BOUND>()
    }

    fn advance_with_automaton<const NO_BOUND: bool>(&mut self) -> bool {
        // fast path, check if prefix always match and we can skip Vec<state> management
        if let Some(always_match_at) = self.always_match_at.take() {
            if !self.advance_delta_reader() {
                return false;
            }
            let common_prefix_len = self.delta_reader.common_prefix_len();
            if always_match_at <= common_prefix_len {
                self.always_match_at = Some(always_match_at);
                return self.reconstruct_key_and_check_upper_bound::<NO_BOUND>();
            }
        } else if !self.advance_delta_reader() {
            return false;
        }

        loop {
            let common_prefix_len = self.delta_reader.common_prefix_len();
            self.states.truncate(common_prefix_len + 1);
            // TODO we could detect when we reach a !can_match, and skip both state and key
            // computation until we truncate that can_t_match out of our state. it's already
            // done at the block layer, so not as important
            let mut state: A::State = self.states.last().unwrap().clone();
            for &b in self.delta_reader.suffix() {
                state = self.automaton.accept(&state, b);
                self.states.push(state.clone());
            }
            let matches = self.automaton.is_match(&state);
            if matches {
                self.always_match_at = self
                    .states
                    .iter()
                    .enumerate()
                    .rev()
                    .take_while(|(_i, state)| self.automaton.will_always_match(state))
                    .last()
                    .map(|(i, _state)| i);
            }

            if !self.reconstruct_key_and_check_upper_bound::<NO_BOUND>() {
                return false;
            }
            if matches {
                return true;
            }
            if !self.advance_delta_reader() {
                return false;
            }
        }
    }

    #[inline(always)]
    fn reconstruct_key_and_check_upper_bound<const NO_BOUND: bool>(&mut self) -> bool {
        let common_prefix_len = self.delta_reader.common_prefix_len();
        self.key
            .update_with_prefix(common_prefix_len, self.delta_reader.suffix());

        // TODO there is an idea where we only look at the upper bound when our delta_reader
        // reached the last block (if we pruned blocks beforehand (do we always?) we cannot
        // find that key before that block)
        NO_BOUND
            || matches_upper_bound(
                &mut self.upper_bound_comparator,
                &self.upper_bound,
                common_prefix_len,
                self.delta_reader.suffix(),
            )
    }

    /// Returns the `TermOrdinal` of the given term.
    ///
    /// May panic if the called as `.advance()` as never
    /// been called before.
    pub fn term_ord(&self) -> TermOrdinal {
        self.term_ord.unwrap_or(0u64)
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
}

impl<TSSTable, A> Streamer<'_, TSSTable, A, Vec<u8>>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
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

    /// Return the next `(key, value)` pair.
    #[expect(clippy::should_implement_trait)]
    #[inline(always)]
    pub fn next(&mut self) -> Option<(&[u8], &TSSTable::Value)> {
        if self.advance() {
            Some((self.key(), self.value()))
        } else {
            None
        }
    }
}

impl<TSSTable, A> Streamer<'_, TSSTable, A, WithoutKeys>
where
    A: Automaton,
    A::State: Clone,
    TSSTable: SSTable,
{
    /// Return the next `(key, value)` pair.
    #[inline(always)]
    pub fn next_without_key(&mut self) -> Option<&TSSTable::Value> {
        if self.advance() {
            Some(self.value())
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

    #[test]
    fn test_sstable_search_without_keys() {
        let term_dict = create_test_dictionary().unwrap();
        let ptn = tantivy_fst::Regex::new("ab.*t.*").unwrap();
        let mut term_streamer = term_dict.search(ptn).without_keys().into_stream().unwrap();
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.term_ord(), 1);
        assert_eq!(term_streamer.value(), &1u64);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.term_ord(), 2);
        assert_eq!(term_streamer.value(), &2u64);
        assert!(!term_streamer.advance());
    }

    #[test]
    fn test_sstable_range_first_key_past_upper_bound() {
        let term_dict = create_test_dictionary().unwrap();
        // "abalation" is the first key past the lower bound, and it already exceeds the upper
        // bound.
        let mut keyed_stream = term_dict
            .range()
            .gt("abaisance")
            .lt("abal")
            .into_stream()
            .unwrap();
        assert!(!keyed_stream.advance());
        let mut keyless_stream = term_dict
            .range()
            .gt("abaisance")
            .lt("abal")
            .without_keys()
            .into_stream()
            .unwrap();
        assert!(!keyless_stream.advance());
    }

    #[test]
    fn test_sstable_range_without_keys() {
        let term_dict = create_test_dictionary().unwrap();
        let mut term_streamer = term_dict
            .range()
            .ge("abal")
            .le("abalienate")
            .without_keys()
            .into_stream()
            .unwrap();
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.term_ord(), 1);
        assert_eq!(term_streamer.value(), &1u64);
        assert!(term_streamer.advance());
        assert_eq!(term_streamer.term_ord(), 2);
        assert_eq!(term_streamer.value(), &2u64);
        assert!(!term_streamer.advance());
    }

    // TODO add test for sparse search with a block of poison (starts with 0xffffffff) => such a
    // block instantly causes an unexpected EOF error
}
