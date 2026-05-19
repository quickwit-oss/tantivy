//! `BitsetFromPostings` strategy for `FastFieldTermSetQuery`.
//!
//! Performs lookups for the sorted query terms in the inverted-index
//! term dictionary and OR's each matching posting list into a single
//! segment-sized `BitSet`. The result is wrapped in `BitSetDocSet` and
//! returned as a scorer.
//!
//! Cost is approximately `K × per_lookup + bitset_iter`. `LinearScan`
//! is preferred when K is a sizable fraction of N — at that point the
//! K lookups outweigh a single O(N) hashset-probing scan. The planner
//! gate (`bitset_max_density_unique` / `bitset_max_density_multi`)
//! admits the bitset region; see `term_set_strategy.rs` for the
//! dispatch and threshold calibration.
//!
//! ## Batched dictionary lookups (quickwit / SSTable backend)
//!
//! Under the SSTable backend (`quickwit` feature), `TermDictionary::get`
//! pays a zstd block decompress per call. For K query terms with
//! multiple-per-block density, K individual calls repeatedly
//! decompress the same blocks. [`TermDictionary::batch_term_info_exact`]
//! walks a *sorted* key list forward through the dictionary, decoding
//! each touched block exactly once. Across the production cell matrix
//! the batched API is multiple× faster than per-key `get`, with
//! larger wins on denser cells (more keys per block).
//!
//! Under the FST backend (no `quickwit` feature), `get` is already
//! cheap and there's no block-decode to amortize, so we keep the
//! original per-key loop.
//!
//! [`TermDictionary::batch_term_info_exact`]: crate::termdict::TermDictionary::batch_term_info_exact
//!
//! ## Why direct lookups, not streaming-with-automaton
//!
//! The streaming alternative would walk the term dictionary with an
//! FST set-membership automaton, touching all K matches plus every
//! intervening entry — per-entry cost scales with `dict_size` (≈ N/D),
//! not K. Microbenchmarks across a wide K/D/N matrix found direct
//! lookups faster on every cell, frequently by 10–25× on low-D
//! columns. The SSTable streamer also lacks within-entry `can_match`
//! early termination, so bytes feed through the automaton even into
//! dead states. Direct lookups skip the walk entirely.

use common::BitSet;

use crate::index::{InvertedIndexReader, SegmentReader};
use crate::postings::TermInfo;
use crate::query::{BitSetDocSet, ConstScorer, EmptyScorer, Scorer};
use crate::schema::{Field, IndexRecordOption};
use crate::Score;

/// Build a `Scorer` that OR's the posting lists of every input key into
/// a per-segment `BitSet`.
///
/// `sorted_keys` must be **sorted ascending in byte order with no
/// duplicates** — this is the same precondition `SortedTermSlice` encodes
/// at the type level, and the quickwit arm wraps `sorted_keys` in
/// `SortedTermSlice::new_assume_sorted` to feed the batched dictionary
/// API. The non-quickwit arm doesn't care about ordering for
/// correctness, but reuses the same slice for consistency.
///
/// The key bytes must already match the dictionary's encoding for the
/// field — for numeric fields this is `Term::serialized_value_bytes`
/// (effectively `u64::to_be_bytes` for u64). The caller (`TermSetWeight`)
/// pre-computes these bytes once at construction so per-segment
/// dispatch is alloc-free.
///
/// Caller must guarantee the field has an inverted index; the function
/// returns an error if `reader.inverted_index(field)` fails.
pub(crate) fn bitset_from_postings_scorer(
    reader: &SegmentReader,
    field: Field,
    sorted_keys: &[Vec<u8>],
    boost: Score,
) -> crate::Result<Box<dyn Scorer>> {
    if sorted_keys.is_empty() || reader.max_doc() == 0 {
        return Ok(Box::new(EmptyScorer));
    }

    let inverted_index = reader.inverted_index(field)?;
    let term_dict = inverted_index.terms();
    let mut bitset = BitSet::with_max_value(reader.max_doc());

    #[cfg(feature = "quickwit")]
    {
        use crate::termdict::SortedTermSlice;

        // The SSTable backend's `get` pays a zstd block decompress per
        // call. The caller has already sorted+deduped the keys; we walk
        // the dictionary in a single forward pass via
        // `batch_term_info_exact`. Each touched block decompresses
        // exactly once regardless of how many query terms land in it.
        let sorted = SortedTermSlice::new_assume_sorted(sorted_keys);
        for result in term_dict.batch_term_info_exact(sorted) {
            let (_idx, term_info) = result?;
            or_term_info_into_bitset(&inverted_index, &term_info, &mut bitset)?;
        }
    }
    #[cfg(not(feature = "quickwit"))]
    {
        // FST backend: `get` is cheap (no block decompression), so the
        // per-key loop is fine.
        for key in sorted_keys {
            let Some(term_info) = term_dict.get(key.as_slice())? else {
                continue;
            };
            or_term_info_into_bitset(&inverted_index, &term_info, &mut bitset)?;
        }
    }

    Ok(Box::new(ConstScorer::new(
        BitSetDocSet::from(bitset),
        boost,
    )))
}

/// Open the posting list for `term_info` and OR every matched doc into
/// `bitset`. Used by both the quickwit (batched) and non-quickwit
/// (per-key) arms above.
fn or_term_info_into_bitset(
    inverted_index: &InvertedIndexReader,
    term_info: &TermInfo,
    bitset: &mut BitSet,
) -> crate::Result<()> {
    let mut block_postings =
        inverted_index.read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
    loop {
        let docs = block_postings.docs();
        if docs.is_empty() {
            break;
        }
        for &doc in docs {
            bitset.insert(doc);
        }
        block_postings.advance();
    }
    Ok(())
}
