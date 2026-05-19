//! Equivalence tests for `Dictionary::batch_term_info_exact`.
//!
//! For each test cell we build a `Dictionary<MonotonicU64SSTable>`, then
//! compare the batched iterator's output against a hand-rolled baseline
//! that does K individual `dict.get(key)` calls. Both must yield the
//! same `(input_index, value)` pairs in the same order.
//!
//! `MonotonicU64SSTable` is enough — the `batch_term_info_exact` algorithm
//! is fully agnostic to the value type (it only does `value.clone()`). If
//! the value-reader plumbing works for `u64`, it works for `TermInfo`.

use std::io;

use common::OwnedBytes;
use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_sstable::{Dictionary, MonotonicU64SSTable, SortedTermSlice, sort_and_dedupe_terms};

/// Build a dictionary of `n` u64-encoded BE keys with values `i as u64`.
/// Optionally override the target block length so we can force more
/// block transitions for small dicts.
fn build_dict(n: usize, block_len: Option<usize>) -> Dictionary<MonotonicU64SSTable> {
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    if let Some(bl) = block_len {
        builder.set_block_len(bl);
    }
    for i in 0..n as u64 {
        let key = i.to_be_bytes();
        builder.insert(&key[..], &i).unwrap();
    }
    let bytes = builder.finish().unwrap();
    Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap()
}

/// Build a dictionary of `n` arbitrary byte-string keys (deterministic
/// per `seed`), with values `i as u64`. Returns the dictionary plus the
/// sorted-deduped key list (which the dictionary preserves).
fn build_dict_strings(n: usize, seed: u64) -> (Dictionary<MonotonicU64SSTable>, Vec<Vec<u8>>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut keys: Vec<Vec<u8>> = (0..n)
        .map(|_| {
            let len = rng.random_range(1..16usize);
            (0..len).map(|_| rng.random()).collect()
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();

    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    for (i, key) in keys.iter().enumerate() {
        builder.insert(&key[..], &(i as u64)).unwrap();
    }
    let bytes = builder.finish().unwrap();
    let dict =
        Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap();
    (dict, keys)
}

/// Compute the baseline `(idx, value)` pairs by calling `dict.get(key)`
/// on each input key.
fn baseline<K: AsRef<[u8]>>(
    dict: &Dictionary<MonotonicU64SSTable>,
    keys: &[K],
) -> Vec<(usize, u64)> {
    keys.iter()
        .enumerate()
        .filter_map(|(i, k)| dict.get(k.as_ref()).unwrap().map(|v| (i, v)))
        .collect()
}

/// Drive the batched iterator and collect the result.
fn batched<K: AsRef<[u8]>>(
    dict: &Dictionary<MonotonicU64SSTable>,
    sorted: SortedTermSlice<'_, K>,
) -> Vec<(usize, u64)> {
    dict.batch_term_info_exact(sorted)
        .collect::<io::Result<Vec<_>>>()
        .unwrap()
}

/// Assert that the batched iterator and `dict.get`-baseline produce the
/// same result on a given sorted input.
fn assert_equivalent_on_sorted(dict: &Dictionary<MonotonicU64SSTable>, sorted_keys: &[Vec<u8>]) {
    let sorted = SortedTermSlice::new(sorted_keys).expect("test input must be sorted");
    let base = baseline(dict, sorted_keys);
    let batch = batched(dict, sorted);
    assert_eq!(
        base,
        batch,
        "batched output diverged from dict.get() baseline (n_keys={})",
        sorted_keys.len()
    );
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn empty_input_yields_none() {
    let dict = build_dict(100, None);
    let keys: Vec<Vec<u8>> = vec![];
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn empty_dictionary_yields_none_for_any_input() {
    let dict = build_dict(0, None);
    let keys: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn all_inputs_past_last_key() {
    // Dict has keys 0..100 (big-endian u64); query keys far past those.
    let dict = build_dict(100, None);
    let keys: Vec<Vec<u8>> = (0u64..5)
        .map(|i| (i + 10_000).to_be_bytes().to_vec())
        .collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn all_inputs_before_first_key() {
    // Dict starts at key `0x01u64` (BE), query keys are all zero-prefixed
    // sub-keys that sort strictly before that.
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    for i in 1u64..50 {
        builder.insert(&i.to_be_bytes()[..], &i).unwrap();
    }
    let bytes = builder.finish().unwrap();
    let dict =
        Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap();

    let keys: Vec<Vec<u8>> = vec![vec![0u8], vec![0, 0]];
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn single_input_present() {
    let dict = build_dict(100, None);
    let keys: Vec<Vec<u8>> = vec![42u64.to_be_bytes().to_vec()];
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn single_input_absent() {
    // Dict has 0..100; query for 999 which is past the last key.
    let dict = build_dict(100, None);
    let keys: Vec<Vec<u8>> = vec![999u64.to_be_bytes().to_vec()];
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn all_present() {
    // Query every dict key.
    let dict = build_dict(500, None);
    let keys: Vec<Vec<u8>> = (0u64..500).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn all_missing_interspersed_with_dict_range() {
    // Dict has even-numbered keys 0, 2, 4, ..., 198 (n=100). Query for
    // the odd keys in the same range — all absent but interleaved with
    // the dictionary keys, so the iterator must Greater-skip per target.
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    for i in 0u64..100 {
        let v = i * 2;
        builder.insert(&v.to_be_bytes()[..], &v).unwrap();
    }
    let bytes = builder.finish().unwrap();
    let dict =
        Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap();

    let keys: Vec<Vec<u8>> = (0u64..100)
        .map(|i| (i * 2 + 1).to_be_bytes().to_vec())
        .collect();
    // The last query (199) is past the dict's max (198), so the
    // `get_block_with_key` short-circuit fires for it; the rest must
    // resolve via Greater-skip within blocks.
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn mixed_present_and_absent_alternating() {
    // Same construction as above; query for both evens (present) and
    // odds (absent), alternating.
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    for i in 0u64..100 {
        let v = i * 2;
        builder.insert(&v.to_be_bytes()[..], &v).unwrap();
    }
    let bytes = builder.finish().unwrap();
    let dict =
        Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap();

    let keys: Vec<Vec<u8>> = (0u64..50).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn inputs_concentrated_in_one_block() {
    // Tiny block_len forces many blocks; pick a query subset clustered
    // in a tight key range so all map to the same block.
    let dict = build_dict(1000, Some(64));
    // 10 consecutive keys starting at 250.
    let keys: Vec<Vec<u8>> = (250u64..260).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn multiple_absent_targets_in_same_exhausted_block() {
    // Sparse dict (keys 0, 10, 20, ..., 190) with `block_len = 2`
    // engineers wide inter-block gaps. The block-index entry for the
    // block containing keys 10 and 20 may pad `last_key_or_greater`
    // forward into the gap, so multiple absent targets in 11..20 can
    // resolve to the same block. After the first such target exhausts
    // the in-block walker, the `block_exhausted` short-circuit must
    // skip the rest without re-entering the walker — output equivalence
    // against the per-key `dict.get` baseline pins this.
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
    builder.set_block_len(2);
    for i in 0u64..20 {
        let v = i * 10;
        builder.insert(&v.to_be_bytes()[..], &v).unwrap();
    }
    let bytes = builder.finish().unwrap();
    let dict =
        Dictionary::<MonotonicU64SSTable>::from_bytes_for_tests(OwnedBytes::new(bytes)).unwrap();
    let keys: Vec<Vec<u8>> = (11u64..20).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

#[test]
fn inputs_spanning_many_blocks() {
    // Tiny block_len + queries spread evenly across the key range
    // exercises the block-transition logic many times.
    let dict = build_dict(1000, Some(64));
    let keys: Vec<Vec<u8>> = (0u64..1000)
        .step_by(37)
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
    assert_equivalent_on_sorted(&dict, &keys);
}

// ---------------------------------------------------------------------------
// Randomized equivalence — proptest with shrinking
// ---------------------------------------------------------------------------

/// Per-query intent. Proptest strategies can't depend on per-run runtime
/// values (`dict_keys` is computed from `dict_n` / `dict_seed`), so we
/// generate "either an index that will resolve to a likely-present key,
/// or fresh bytes that are almost certainly absent" and resolve to
/// actual `Vec<u8>` keys in the test body. This keeps the
/// mixed-presence property of the old seeded helpers while letting
/// proptest shrink on either dimension when a failure surfaces.
#[derive(Debug, Clone)]
enum QuerySpec {
    /// Index into the generated `dict_keys`, mod its length at use site.
    /// Resolves to a key that is definitely present in the dictionary.
    DictIndex(usize),
    /// Fresh bytes. Overwhelmingly likely to be absent — proptest can
    /// shrink the byte vector when a failure depends on its shape.
    Bytes(Vec<u8>),
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

    /// Randomized equivalence across dictionary sizes and query shapes.
    /// Replaces the older fixed-seed `random_*` battery — proptest
    /// covers the same space and shrinks failing inputs to a minimal
    /// counterexample on regression.
    #[test]
    fn batch_equivalent_to_baseline_proptest(
        dict_n in 1usize..2_000,
        dict_seed in 0u64..1_000_000,
        query_specs in prop::collection::vec(
            prop_oneof![
                any::<usize>().prop_map(QuerySpec::DictIndex),
                prop::collection::vec(any::<u8>(), 1..32usize).prop_map(QuerySpec::Bytes),
            ],
            0..200usize,
        ),
    ) {
        let (dict, dict_keys) = build_dict_strings(dict_n, dict_seed);
        let mut query: Vec<Vec<u8>> = query_specs
            .into_iter()
            .filter_map(|spec| match spec {
                QuerySpec::DictIndex(i) if !dict_keys.is_empty() => {
                    Some(dict_keys[i % dict_keys.len()].clone())
                }
                QuerySpec::DictIndex(_) => None,
                QuerySpec::Bytes(b) => Some(b),
            })
            .collect();
        sort_and_dedupe_terms(&mut query);

        let sorted = SortedTermSlice::new(&query).expect("sorted+deduped by construction");
        let base = baseline(&dict, &query);
        let batch = batched(&dict, sorted);
        prop_assert_eq!(base, batch);
    }
}
