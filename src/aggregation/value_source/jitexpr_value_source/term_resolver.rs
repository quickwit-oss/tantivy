//! Resolves the term ords of a text input into terms.

use std::sync::Arc;

use columnar::Dictionary;
use rustc_hash::FxHashMap;

use crate::{TantivyError, COLLECT_BLOCK_BUFFER_LEN};

const MISSING_TERM_ORD_SENTINEL: usize = usize::MAX;
/// Dictionaries with at most this many terms are decoded entirely when the column is opened.
const FULL_DECODE_MAX_NUM_TERMS: usize = 1024;
/// The cache of decoded terms is cleared when its terms exceed this many bytes...
const CACHE_MAX_NUM_BYTES: usize = 1_000_000; // 1mb
/// ... or when it holds more than this many terms.
const CACHE_MAX_NUM_TERMS: usize = 64_000_000; // 64kb

/// Resolves the term ords of a block into terms.
pub(super) enum TermResolver {
    /// Not a text input.
    None,
    /// All of the terms of the dictionary, decoded upfront. The arena index is the term ord.
    Full(TermArena),
    /// Terms decoded on demand, cached across blocks.
    Cached(CachedTerms),
}

impl TermResolver {
    /// Decodes the whole dictionary if it is small, and decodes terms on demand otherwise.
    pub(super) fn for_dictionary(term_dictionary: Arc<Dictionary>) -> crate::Result<TermResolver> {
        if term_dictionary.num_terms() <= FULL_DECODE_MAX_NUM_TERMS {
            let all_terms = decode_all_terms(&term_dictionary)?;
            return Ok(TermResolver::Full(all_terms));
        }
        Ok(TermResolver::Cached(CachedTerms::new(
            term_dictionary,
            CACHE_MAX_NUM_BYTES,
            CACHE_MAX_NUM_TERMS,
        )))
    }

    /// Prepares the terms of `term_ords`, the term ords of a block.
    ///
    /// Panics if the dictionary is corrupted: `load_block` cannot return an error.
    pub(super) fn load(&mut self, term_ords: &[Option<u64>]) {
        if let TermResolver::Cached(cached_terms) = self {
            cached_terms.load(term_ords);
        }
    }

    /// Returns the term of `term_ord`, the term ord of the doc at `doc_pos` in the last block.
    ///
    /// Precondition: `term_ords[doc_pos] == Some(term_ord)` for the last `load` call.
    #[inline]
    pub(super) fn term(&self, doc_pos: usize, term_ord: u64) -> &str {
        match self {
            TermResolver::None => unreachable!("only text inputs have terms"),
            TermResolver::Full(all_terms) => all_terms.get(term_ord as usize),
            TermResolver::Cached(cached_terms) => cached_terms.term(doc_pos),
        }
    }

    /// Returns the term of `term_ord`, outside of the block path.
    ///
    /// Invalidates the last `load` call: `term` must not be called before the next `load`.
    ///
    /// Panics if the dictionary is corrupted.
    #[inline]
    pub(super) fn term_for_ord(&mut self, term_ord: u64) -> &str {
        match self {
            TermResolver::None => unreachable!("only text inputs have terms"),
            TermResolver::Full(all_terms) => all_terms.get(term_ord as usize),
            TermResolver::Cached(cached_terms) => cached_terms.term_for_ord(term_ord),
        }
    }
}

/// Decodes all of the terms of `term_dictionary`. The term of term ord `i` gets index `i`.
fn decode_all_terms(term_dictionary: &Dictionary) -> crate::Result<TermArena> {
    let num_terms = term_dictionary.num_terms();
    let mut all_terms = TermArena::with_num_terms(num_terms);
    // The stream returns the terms in term ord order, starting at 0.
    let mut term_stream = term_dictionary.stream()?;
    while term_stream.advance() {
        let term = std::str::from_utf8(term_stream.key()).map_err(|_| corrupted_dictionary())?;
        all_terms.push(term);
    }
    if all_terms.num_terms() != num_terms {
        return Err(corrupted_dictionary());
    }
    Ok(all_terms)
}

fn corrupted_dictionary() -> TantivyError {
    TantivyError::InternalError("fast-field string dictionary is corrupted".to_string())
}

/// Terms concatenated in a single buffer.
pub(super) struct TermArena {
    buffer: String,
    /// `buffer[offsets[idx]..offsets[idx + 1]]` is the term of index `idx`.
    ///
    /// Invariant: `offsets[0] == 0`, and `offsets.len() == num_terms() + 1`.
    offsets: Vec<usize>,
}

impl TermArena {
    fn with_num_terms(num_terms: usize) -> TermArena {
        let mut offsets = Vec::with_capacity(num_terms + 1);
        offsets.push(0);
        TermArena {
            buffer: String::new(),
            offsets,
        }
    }

    fn num_terms(&self) -> usize {
        self.offsets.len() - 1
    }

    fn num_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Appends `term` and returns its index.
    fn push(&mut self, term: &str) -> usize {
        let idx = self.num_terms();
        self.buffer.push_str(term);
        self.offsets.push(self.buffer.len());
        idx
    }

    #[inline]
    fn get(&self, idx: usize) -> &str {
        &self.buffer[self.offsets[idx]..self.offsets[idx + 1]]
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.offsets.truncate(1);
    }
}

/// Terms decoded on demand, cached across blocks.
///
/// The cache is cleared when it gets too large, before loading a block. A block has at most
/// `COLLECT_BLOCK_BUFFER_LEN` distinct term ords, so the cache size is bounded by the limits plus
/// one block.
pub(super) struct CachedTerms {
    term_dictionary: Arc<Dictionary>,
    /// The cache is cleared when `arena.num_bytes() > max_num_bytes`...
    max_num_bytes: usize,
    /// ... or when `ord_to_idx.len() > max_num_terms`.
    max_num_terms: usize,
    /// Term ord -> index of its term in `arena`.
    ord_to_idx: FxHashMap<u64, usize>,
    arena: TermArena,
    /// Scratch buffer: the term ords of the block missing from the cache.
    missing_ords: Vec<u64>,
    /// Scratch buffer: the bytes of a term decoded by `term_for_ord`.
    term_bytes: Vec<u8>,
    /// `doc_term_idx[doc_pos]` is the index in `arena` of the term of the doc at `doc_pos` in the
    /// last block. Meaningless for the docs without a value.
    doc_term_idx: Vec<usize>,
}

impl CachedTerms {
    fn new(
        term_dictionary: Arc<Dictionary>,
        max_num_bytes: usize,
        max_num_terms: usize,
    ) -> CachedTerms {
        CachedTerms {
            term_dictionary,
            max_num_bytes,
            max_num_terms,
            ord_to_idx: FxHashMap::default(),
            arena: TermArena::with_num_terms(0),
            missing_ords: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
            term_bytes: Vec::new(),
            doc_term_idx: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
        }
    }

    /// Clears the cache if it exceeds one of its limits.
    fn evict_if_too_large(&mut self) {
        if self.arena.num_bytes() > self.max_num_bytes || self.ord_to_idx.len() > self.max_num_terms
        {
            self.ord_to_idx.clear();
            self.arena.clear();
        }
    }

    /// Panics if the dictionary is corrupted.
    fn load(&mut self, term_ords: &[Option<u64>]) {
        // Evicting before resolving the block ensures that all of its terms are in the cache.
        self.evict_if_too_large();
        let CachedTerms {
            term_dictionary,
            ord_to_idx,
            arena,
            missing_ords,
            doc_term_idx,
            ..
        } = self;
        missing_ords.clear();
        // identify the term ord that are not in cache.
        for &term_ord in term_ords.iter().flatten() {
            if !ord_to_idx.contains_key(&term_ord) {
                missing_ords.push(term_ord);
            }
        }
        // If we do have missing term_ord, we need to populate those from the cache.
        if !missing_ords.is_empty() {
            missing_ords.sort_unstable();
            missing_ords.dedup();
            // The callback is called once per ord of `missing_ords`, in order: they are deduped.
            let mut missing_ords_iter = missing_ords.iter();
            let all_found = term_dictionary
                .sorted_ords_to_term_cb(missing_ords, |term| {
                    let term_str = std::str::from_utf8(term)
                        .expect("fast-field string dictionary is corrupted");
                    let term_ord = *missing_ords_iter
                        .next()
                        .expect("one callback call per missing ord");
                    let idx = arena.push(term_str);
                    ord_to_idx.insert(term_ord, idx);
                })
                .expect("fast-field string dictionary is corrupted");
            assert!(all_found, "fast-field string dictionary is corrupted");
        }
        doc_term_idx.clear();
        for term_ord_opt in term_ords {
            let idx: usize = match term_ord_opt {
                Some(term_ord) => ord_to_idx[term_ord],
                None => MISSING_TERM_ORD_SENTINEL,
            };
            doc_term_idx.push(idx);
        }
    }

    /// Precondition: the doc at `doc_pos` in the last block has a value.
    #[inline]
    fn term(&self, doc_pos: usize) -> &str {
        self.arena.get(self.doc_term_idx[doc_pos])
    }

    /// Returns the term of `term_ord`, decoding it if it is not in the cache.
    ///
    /// May evict the cache: invalidates the last `load` call.
    ///
    /// Panics if the dictionary is corrupted.
    fn term_for_ord(&mut self, term_ord: u64) -> &str {
        self.evict_if_too_large();
        let idx: usize = match self.ord_to_idx.get(&term_ord) {
            Some(&idx) => idx,
            None => {
                self.term_bytes.clear();
                let found = self
                    .term_dictionary
                    .ord_to_term(term_ord, &mut self.term_bytes)
                    .expect("fast-field string dictionary is corrupted");
                assert!(found, "fast-field string dictionary is corrupted");
                let term_str = std::str::from_utf8(&self.term_bytes)
                    .expect("fast-field string dictionary is corrupted");
                let idx = self.arena.push(term_str);
                self.ord_to_idx.insert(term_ord, idx);
                idx
            }
        };
        self.arena.get(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::FileSlice;

    fn build_dictionary(terms: &[String]) -> Arc<Dictionary> {
        let mut builder = <Dictionary>::builder(Vec::new()).unwrap();
        for term in terms {
            builder.insert(term.as_bytes(), &()).unwrap();
        }
        let dictionary_bytes: Vec<u8> = builder.finish().unwrap();
        Arc::new(Dictionary::open(FileSlice::from(dictionary_bytes)).unwrap())
    }

    /// Sorted terms, so that the term ord of `terms[i]` is `i`.
    fn sorted_terms(num_terms: usize) -> Vec<String> {
        (0..num_terms).map(|i| format!("term{i:05}")).collect()
    }

    /// Loads `term_ords` and checks the term of each doc with a value.
    fn check_block(term_resolver: &mut TermResolver, terms: &[String], term_ords: &[Option<u64>]) {
        term_resolver.load(term_ords);
        for (doc_pos, term_ord_opt) in term_ords.iter().enumerate() {
            if let Some(term_ord) = *term_ord_opt {
                assert_eq!(
                    term_resolver.term(doc_pos, term_ord),
                    terms[term_ord as usize]
                );
            }
        }
    }

    #[test]
    fn test_small_dictionary_is_decoded_upfront() {
        let terms = sorted_terms(10);
        let mut term_resolver = TermResolver::for_dictionary(build_dictionary(&terms)).unwrap();
        assert!(matches!(term_resolver, TermResolver::Full(_)));
        check_block(
            &mut term_resolver,
            &terms,
            &[Some(3), None, Some(0), Some(3)],
        );
        check_block(&mut term_resolver, &terms, &[Some(9), Some(1), None]);
    }

    #[test]
    fn test_empty_dictionary() {
        let mut term_resolver = TermResolver::for_dictionary(build_dictionary(&[])).unwrap();
        assert!(matches!(term_resolver, TermResolver::Full(_)));
        check_block(&mut term_resolver, &[], &[None, None]);
    }

    #[test]
    fn test_large_dictionary_is_cached() {
        let terms = sorted_terms(3000);
        let mut term_resolver = TermResolver::for_dictionary(build_dictionary(&terms)).unwrap();
        assert!(matches!(term_resolver, TermResolver::Cached(_)));
        check_block(
            &mut term_resolver,
            &terms,
            &[Some(2999), None, Some(0), Some(1500), Some(0)],
        );
        // Reuses cached ords, and adds new ones.
        check_block(
            &mut term_resolver,
            &terms,
            &[Some(1500), Some(7), None, Some(2999)],
        );
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 4);
        assert_eq!(cached_terms.arena.num_terms(), 4);
    }

    #[test]
    fn test_cache_eviction() {
        let terms = sorted_terms(3000);
        let cached_terms = CachedTerms::new(build_dictionary(&terms), usize::MAX, 2);
        let mut term_resolver = TermResolver::Cached(cached_terms);
        // A single block larger than the limit.
        check_block(
            &mut term_resolver,
            &terms,
            &[Some(1), Some(2), Some(3), Some(4)],
        );
        // The cache is cleared before this block.
        check_block(&mut term_resolver, &terms, &[Some(4), Some(5)]);
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 2);
        // Under the limit: no eviction.
        check_block(&mut term_resolver, &terms, &[Some(5), Some(2000)]);
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 3);
        // Byte limit.
        let cached_terms = CachedTerms::new(build_dictionary(&terms), 10, usize::MAX);
        let mut term_resolver = TermResolver::Cached(cached_terms);
        check_block(&mut term_resolver, &terms, &[Some(10), Some(11)]);
        check_block(&mut term_resolver, &terms, &[Some(12), Some(10)]);
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 2);
    }

    #[test]
    fn test_term_for_ord_full() {
        let terms = sorted_terms(10);
        let mut term_resolver = TermResolver::for_dictionary(build_dictionary(&terms)).unwrap();
        assert!(matches!(term_resolver, TermResolver::Full(_)));
        for term_ord in [3u64, 0, 9, 3] {
            assert_eq!(
                term_resolver.term_for_ord(term_ord),
                terms[term_ord as usize]
            );
        }
    }

    #[test]
    fn test_term_for_ord_cached() {
        let terms = sorted_terms(3000);
        let cached_terms = CachedTerms::new(build_dictionary(&terms), usize::MAX, 2);
        let mut term_resolver = TermResolver::Cached(cached_terms);
        for term_ord in [1500u64, 7, 1500] {
            assert_eq!(
                term_resolver.term_for_ord(term_ord),
                terms[term_ord as usize]
            );
        }
        // `1500` is a cache hit.
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 2);
        assert_eq!(cached_terms.arena.num_terms(), 2);
        // At the limit: no eviction. The cache goes over the limit.
        assert_eq!(term_resolver.term_for_ord(2999), terms[2999]);
        // Over the limit: the cache is cleared before decoding.
        assert_eq!(term_resolver.term_for_ord(0), terms[0]);
        let TermResolver::Cached(cached_terms) = &term_resolver else {
            panic!("expected cached terms");
        };
        assert_eq!(cached_terms.ord_to_idx.len(), 1);
        // The block path still works after the single ord path.
        check_block(
            &mut term_resolver,
            &terms,
            &[Some(1), None, Some(2500), Some(1)],
        );
    }
}
