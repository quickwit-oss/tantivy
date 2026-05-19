//! Batched exact-key dictionary lookups.
//!
//! `Dictionary::get(key)` decompresses one SSTable block per call. For a
//! large `WHERE col IN (v1, v2, ..., vK)` filter pushed down as
//! `BitsetFromPostings`, the K individual `get()` calls repeatedly
//! decompress the same blocks when multiple query terms fall within the
//! same block.
//!
//! This module provides:
//!
//!   - [`SortedTermSlice`], the input contract that enforces "sorted ascending byte-order, no
//!     duplicates" at the type level.
//!   - [`sort_and_dedupe_terms`], an in-place helper for callers whose input is not yet sorted.
//!   - [`BatchedTermInfoIter`], an iterator that walks a `SortedTermSlice` through the dictionary
//!     in a single forward pass, decoding each touched block exactly once. Structurally a mirror of
//!     [`Dictionary::sorted_ords_to_term_cb`] going `sorted keys → (input_index, value)` instead of
//!     `sorted ords → key`.
//!
//! [`Dictionary::sorted_ords_to_term_cb`]: crate::Dictionary::sorted_ords_to_term_cb

use std::cmp::Ordering;
use std::io;
use std::marker::PhantomData;

use crate::dictionary::Dictionary;
use crate::{BlockAddr, DeltaReader, SSTable};

/// A slice of byte-encoded terms guaranteed to be sorted strictly
/// ascending in byte order with no duplicates.
///
/// Used as the input contract for [`Dictionary::batch_term_info_exact`].
/// Construct via [`SortedTermSlice::new`] (validating, O(n)) or
/// [`SortedTermSlice::new_assume_sorted`] (debug-assert only, O(1)).
///
/// [`Dictionary::batch_term_info_exact`]: crate::Dictionary::batch_term_info_exact
pub struct SortedTermSlice<'a, K: AsRef<[u8]> = &'a [u8]> {
    inner: &'a [K],
    _phantom: PhantomData<&'a K>,
}

impl<'a, K: AsRef<[u8]>> SortedTermSlice<'a, K> {
    /// Construct after validating the input is strictly ascending in byte
    /// order with no duplicates.
    ///
    /// Cost: O(n) validation pass. Returns `None` if any adjacent pair is
    /// out of order or equal.
    pub fn new(terms: &'a [K]) -> Option<Self> {
        if terms.windows(2).all(|w| w[0].as_ref() < w[1].as_ref()) {
            Some(Self {
                inner: terms,
                _phantom: PhantomData,
            })
        } else {
            None
        }
    }

    /// Construct without validation. In debug builds, the strict-ascending
    /// precondition is checked via `debug_assert!`. In release builds the
    /// caller is trusted; violating the precondition produces silently
    /// incorrect lookup results (not undefined behaviour).
    ///
    /// Cost: O(0) in release, O(n) debug-assert pass in debug.
    pub fn new_assume_sorted(terms: &'a [K]) -> Self {
        debug_assert!(
            terms.windows(2).all(|w| w[0].as_ref() < w[1].as_ref()),
            "SortedTermSlice::new_assume_sorted: terms must be strictly ascending in byte order \
             with no duplicates"
        );
        Self {
            inner: terms,
            _phantom: PhantomData,
        }
    }

    pub fn as_slice(&self) -> &[K] {
        self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Sort a `Vec` of byte-encoded terms in place by byte order and remove
/// duplicates. The output satisfies [`SortedTermSlice`]'s precondition,
/// so callers can pass `&v` to [`SortedTermSlice::new_assume_sorted`]
/// without re-validating.
pub fn sort_and_dedupe_terms<K: AsRef<[u8]> + Ord>(terms: &mut Vec<K>) {
    terms.sort_unstable();
    terms.dedup();
}

/// Iterator over batched dictionary lookups for sorted exact keys.
///
/// Constructed via [`Dictionary::batch_term_info_exact`]. Yields
/// `Ok((input_index, value))` for each input key present in the
/// dictionary in input order; absent keys are silently skipped. I/O
/// errors during block decode propagate via `Err(_)`; the iterator is
/// fused on error (returns `None` on all subsequent calls).
///
/// Algorithm: walk the input cursor forward through `sorted_keys`. For
/// each target, locate its block via the SSTable index; if that block is
/// not currently loaded, open it (one decompression per touched block).
/// Then advance the in-block delta reader forward, comparing each
/// reconstructed entry key against the target:
///
///   - equal       → emit `(input_cursor, value)`, consume the entry, advance the cursor;
///   - less        → target sorts after; advance the reader and retry;
///   - greater     → target is absent (the dict is sorted, we walked past it); advance the cursor
///     but keep the entry (it may equal the next target).
///
/// If the block exhausts without finding the target, the target is
/// absent in this block; advance the cursor (the next target may live
/// in a different block).
///
/// [`Dictionary::batch_term_info_exact`]: crate::Dictionary::batch_term_info_exact
pub struct BatchedTermInfoIter<'a, K: AsRef<[u8]>, TSSTable: SSTable> {
    dict: &'a Dictionary<TSSTable>,
    sorted_keys: SortedTermSlice<'a, K>,
    input_cursor: usize,
    current_block_addr: Option<BlockAddr>,
    delta_reader: Option<DeltaReader<TSSTable::ValueReader>>,
    current_entry_key: Vec<u8>,
    entry_loaded: bool,
    /// Set when the current block's `DeltaReader` returned `Ok(false)`
    /// (exhausted). Any subsequent input target that maps to the same
    /// block is guaranteed absent and is short-circuited without
    /// re-entering the inner walker, which would otherwise pay another
    /// `advance() -> Ok(false)` per target. Reset on block transition.
    block_exhausted: bool,
    errored: bool,
}

impl<'a, K: AsRef<[u8]>, TSSTable: SSTable> BatchedTermInfoIter<'a, K, TSSTable> {
    pub(crate) fn new(dict: &'a Dictionary<TSSTable>, sorted_keys: SortedTermSlice<'a, K>) -> Self {
        Self {
            dict,
            sorted_keys,
            input_cursor: 0,
            current_block_addr: None,
            delta_reader: None,
            current_entry_key: Vec::new(),
            entry_loaded: false,
            block_exhausted: false,
            errored: false,
        }
    }
}

impl<K, TSSTable> Iterator for BatchedTermInfoIter<'_, K, TSSTable>
where
    K: AsRef<[u8]>,
    TSSTable: SSTable,
{
    type Item = io::Result<(usize, TSSTable::Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }

        loop {
            // Outer: pick the next input target.
            if self.input_cursor >= self.sorted_keys.len() {
                return None;
            }
            let target = self.sorted_keys.as_slice()[self.input_cursor].as_ref();

            // Locate the block that *would* contain `target`. `None` means
            // `target` sorts past the last block; since input is sorted,
            // every remaining input is also past the last block — `?`
            // short-circuits the whole iterator in that case.
            let target_block = self.dict.sstable_index.get_block_with_key(target)?;

            // Transition to a new block if needed. `BlockAddr` derives
            // `PartialEq`, so direct comparison is correct.
            if self.current_block_addr.as_ref() != Some(&target_block) {
                match self.dict.sstable_delta_reader_block(target_block.clone()) {
                    Ok(reader) => {
                        self.delta_reader = Some(reader);
                        self.current_block_addr = Some(target_block);
                        self.current_entry_key.clear();
                        self.entry_loaded = false;
                        self.block_exhausted = false;
                    }
                    Err(e) => {
                        self.errored = true;
                        return Some(Err(e));
                    }
                }
            } else if self.block_exhausted {
                // Same block as the previously-exhausted one. Input is
                // sorted, so this target is past the block's last entry
                // (or the block index padded `last_key_or_greater`
                // forward into a gap). Either way the target is absent;
                // skip without re-entering the inner walker.
                self.input_cursor += 1;
                continue;
            }

            // Inner: walk the current block forward, comparing to target.
            let reader = self
                .delta_reader
                .as_mut()
                .expect("delta reader initialised after block transition");
            loop {
                // Ensure we have a current entry to compare against.
                if !self.entry_loaded {
                    match reader.advance() {
                        Ok(true) => {
                            self.current_entry_key.truncate(reader.common_prefix_len());
                            self.current_entry_key.extend_from_slice(reader.suffix());
                            self.entry_loaded = true;
                        }
                        Ok(false) => {
                            // Block exhausted — target wasn't in this block.
                            // Skip target; the next outer iteration may
                            // transition to a different block via
                            // `get_block_with_key`. Flag the exhaustion
                            // so subsequent targets that resolve to this
                            // same block short-circuit at the outer loop
                            // rather than re-paying an `advance()` here.
                            self.block_exhausted = true;
                            self.input_cursor += 1;
                            break;
                        }
                        Err(e) => {
                            self.errored = true;
                            return Some(Err(e));
                        }
                    }
                }

                match self.current_entry_key.as_slice().cmp(target) {
                    Ordering::Equal => {
                        let value = reader.value().clone();
                        let idx = self.input_cursor;
                        self.input_cursor += 1;
                        // Consume the matched entry; next iteration must
                        // advance the reader before comparing again.
                        self.entry_loaded = false;
                        return Some(Ok((idx, value)));
                    }
                    Ordering::Less => {
                        // Current entry sorts before target — advance.
                        self.entry_loaded = false;
                    }
                    Ordering::Greater => {
                        // Walked past target without finding it. Target is
                        // absent from the dictionary. Skip target; do NOT
                        // advance the reader — the same entry may equal a
                        // later input target.
                        self.input_cursor += 1;
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_term_slice_new_accepts_strictly_ascending() {
        let terms: &[&[u8]] = &[b"a", b"b", b"c"];
        let s = SortedTermSlice::new(terms).expect("strictly ascending");
        assert_eq!(s.len(), 3);
        assert!(!s.is_empty());
        assert_eq!(s.as_slice(), terms);
    }

    #[test]
    fn sorted_term_slice_new_accepts_empty() {
        let terms: &[&[u8]] = &[];
        let s = SortedTermSlice::new(terms).expect("empty is trivially sorted");
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn sorted_term_slice_new_accepts_single_element() {
        let terms: &[&[u8]] = &[b"hello"];
        let s = SortedTermSlice::new(terms).expect("single element is trivially sorted");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn sorted_term_slice_new_rejects_unsorted() {
        let terms: &[&[u8]] = &[b"b", b"a"];
        assert!(SortedTermSlice::new(terms).is_none());
    }

    #[test]
    fn sorted_term_slice_new_rejects_duplicates() {
        let terms: &[&[u8]] = &[b"a", b"a", b"b"];
        assert!(SortedTermSlice::new(terms).is_none());
    }

    #[test]
    fn sorted_term_slice_new_rejects_adjacent_equal() {
        let terms: &[&[u8]] = &[b"abc", b"abc"];
        assert!(SortedTermSlice::new(terms).is_none());
    }

    #[test]
    fn sorted_term_slice_new_assume_sorted_works_on_valid_input() {
        let terms: &[&[u8]] = &[b"a", b"b", b"c"];
        let s = SortedTermSlice::new_assume_sorted(terms);
        assert_eq!(s.len(), 3);
    }

    #[test]
    #[should_panic(expected = "must be strictly ascending")]
    fn sorted_term_slice_new_assume_sorted_panics_on_unsorted_in_debug() {
        let terms: &[&[u8]] = &[b"b", b"a"];
        let _ = SortedTermSlice::new_assume_sorted(terms);
    }

    #[test]
    #[should_panic(expected = "must be strictly ascending")]
    fn sorted_term_slice_new_assume_sorted_panics_on_duplicates_in_debug() {
        let terms: &[&[u8]] = &[b"a", b"a"];
        let _ = SortedTermSlice::new_assume_sorted(terms);
    }

    #[test]
    fn sort_and_dedupe_terms_empty() {
        let mut v: Vec<&[u8]> = vec![];
        sort_and_dedupe_terms(&mut v);
        assert!(v.is_empty());
    }

    #[test]
    fn sort_and_dedupe_terms_single() {
        let mut v: Vec<&[u8]> = vec![b"alpha"];
        sort_and_dedupe_terms(&mut v);
        assert_eq!(v, vec![b"alpha".as_slice()]);
    }

    #[test]
    fn sort_and_dedupe_terms_already_sorted_unique() {
        let mut v: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        sort_and_dedupe_terms(&mut v);
        assert_eq!(v, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn sort_and_dedupe_terms_reverse_sorted() {
        let mut v: Vec<&[u8]> = vec![b"c", b"b", b"a"];
        sort_and_dedupe_terms(&mut v);
        assert_eq!(v, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn sort_and_dedupe_terms_with_duplicates() {
        let mut v: Vec<&[u8]> = vec![b"b", b"a", b"b", b"c", b"a", b"c"];
        sort_and_dedupe_terms(&mut v);
        assert_eq!(v, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn sort_and_dedupe_terms_all_duplicates() {
        let mut v: Vec<&[u8]> = vec![b"x", b"x", b"x", b"x"];
        sort_and_dedupe_terms(&mut v);
        assert_eq!(v, vec![b"x".as_slice()]);
    }
}
