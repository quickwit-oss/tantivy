use std::collections::BTreeSet;
use std::hint::black_box;
use std::io;

use common::file_slice::FileSlice;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_fst::{Automaton, Regex};
use tantivy_sstable::{Dictionary, MonotonicU64SSTable, common_prefix_len};

const CHARSET: &[u8] = b"abcdefghij";
const AUTOMATON_PREFIX: &[u8] = b"ab";
const NUM_AUTOMATON_MATCHES: usize = 1_017;

// Matches `prefix.*`, but only implement can_match/will_always_match if configured to
//
// this allow comparing effects of optimisations depending on these functions
struct HintedPrefixAutomaton<'a> {
    prefix: &'a [u8],
    can_match_hint: bool,
    always_match_hint: bool,
}

impl<'a> HintedPrefixAutomaton<'a> {
    fn new(prefix: &'a [u8], can_match_hint: bool, always_match_hint: bool) -> Self {
        Self {
            prefix,
            can_match_hint,
            always_match_hint,
        }
    }
}

impl Automaton for HintedPrefixAutomaton<'_> {
    type State = Option<usize>;

    fn start(&self) -> Self::State {
        Some(0)
    }

    fn is_match(&self, state: &Self::State) -> bool {
        *state == Some(self.prefix.len())
    }

    fn can_match(&self, state: &Self::State) -> bool {
        !self.can_match_hint || state.is_some()
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        self.always_match_hint && self.is_match(state)
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        let Some(pos) = *state else { return None };
        if pos == self.prefix.len() {
            return Some(pos);
        }
        if self.prefix[pos] == byte {
            Some(pos + 1)
        } else {
            None
        }
    }
}

fn generate_key(rng: &mut impl Rng) -> String {
    let len = rng.random_range(3..12);
    std::iter::from_fn(|| {
        let idx = rng.random_range(0..CHARSET.len());
        Some(CHARSET[idx] as char)
    })
    .take(len)
    .collect()
}

fn prepare_sstable() -> io::Result<Dictionary<MonotonicU64SSTable>> {
    let mut rng = StdRng::from_seed([3u8; 32]);
    let mut els = BTreeSet::new();
    while els.len() < 100_000 {
        els.insert(generate_key(&mut rng));
    }
    let mut dictionary_builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new())?;
    for (ord, word) in els.iter().enumerate() {
        dictionary_builder.insert(word, &(ord as u64))?;
    }
    let buffer = dictionary_builder.finish()?;
    let dictionary = Dictionary::open(FileSlice::from(buffer))?;
    Ok(dictionary)
}

fn stream_bench(
    dictionary: &Dictionary<MonotonicU64SSTable>,
    lower: &[u8],
    upper: &[u8],
    do_scan: bool,
) -> usize {
    let mut stream = dictionary
        .range()
        .ge(lower)
        .lt(upper)
        .into_stream()
        .unwrap();
    if !do_scan {
        return 0;
    }
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

fn automaton_bench(
    dictionary: &Dictionary<MonotonicU64SSTable>,
    can_match_hint: bool,
    always_match_hint: bool,
) -> usize {
    let mut stream = dictionary
        .search(HintedPrefixAutomaton::new(
            AUTOMATON_PREFIX,
            black_box(can_match_hint),
            black_box(always_match_hint),
        ))
        .into_stream()
        .unwrap();
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

fn stream_bench_without_keys(
    dictionary: &Dictionary<MonotonicU64SSTable>,
    lower: &[u8],
    upper: &[u8],
) -> usize {
    let mut stream = dictionary
        .range()
        .ge(lower)
        .lt(upper)
        .without_keys()
        .into_stream()
        .unwrap();
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

fn automaton_bench_without_keys(
    dictionary: &Dictionary<MonotonicU64SSTable>,
    can_match_hint: bool,
    always_match_hint: bool,
) -> usize {
    let mut stream = dictionary
        .search(HintedPrefixAutomaton::new(
            AUTOMATON_PREFIX,
            black_box(can_match_hint),
            black_box(always_match_hint),
        ))
        .without_keys()
        .into_stream()
        .unwrap();
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

fn regex_bench(dictionary: &Dictionary<MonotonicU64SSTable>, regex: &Regex) -> usize {
    let mut stream = dictionary.search(regex).into_stream().unwrap();
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

fn regex_bench_without_keys(dictionary: &Dictionary<MonotonicU64SSTable>, regex: &Regex) -> usize {
    let mut stream = dictionary
        .search(regex)
        .without_keys()
        .into_stream()
        .unwrap();
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let dict = prepare_sstable().unwrap();
    c.bench_function("short_scan_init", |b| {
        b.iter(|| stream_bench(&dict, b"fa", b"fana", false))
    });
    c.bench_function("short_scan_init_and_scan", |b| {
        b.iter(|| {
            assert_eq!(stream_bench(&dict, b"fa", b"faz", true), 1051);
        })
    });
    c.bench_function("full_scan_init_and_scan_full_with_bound", |b| {
        b.iter(|| {
            assert_eq!(stream_bench(&dict, b"", b"z", true), 100_000);
        })
    });
    c.bench_function("full_scan_init_and_scan_full_no_bounds", |b| {
        b.iter(|| {
            let mut stream = dict.stream().unwrap();
            let mut count = 0;
            while stream.advance() {
                count += 1;
            }
            count
        })
    });
    c.bench_function("full_scan_prefix_automaton_no_hints", |b| {
        b.iter(|| assert_eq!(automaton_bench(&dict, false, false), NUM_AUTOMATON_MATCHES))
    });
    c.bench_function("full_scan_prefix_automaton_can_match_hint_only", |b| {
        b.iter(|| assert_eq!(automaton_bench(&dict, true, false), NUM_AUTOMATON_MATCHES))
    });
    c.bench_function("full_scan_prefix_automaton_always_match_hint_only", |b| {
        b.iter(|| assert_eq!(automaton_bench(&dict, false, true), NUM_AUTOMATON_MATCHES))
    });
    c.bench_function("full_scan_prefix_automaton_both_hints", |b| {
        b.iter(|| assert_eq!(automaton_bench(&dict, true, true), NUM_AUTOMATON_MATCHES))
    });

    c.bench_function(
        "full_scan_init_and_scan_full_with_bound_without_keys",
        |b| {
            b.iter(|| {
                assert_eq!(stream_bench_without_keys(&dict, b"", b"z"), 100_000);
            })
        },
    );
    c.bench_function("full_scan_init_and_scan_full_no_bounds_without_keys", |b| {
        b.iter(|| {
            let mut stream = dict.range().without_keys().into_stream().unwrap();
            let mut count = 0;
            while stream.advance() {
                count += 1;
            }
            count
        })
    });
    c.bench_function("full_scan_prefix_automaton_no_hints_without_keys", |b| {
        b.iter(|| {
            assert_eq!(
                automaton_bench_without_keys(&dict, false, false),
                NUM_AUTOMATON_MATCHES
            )
        })
    });
    c.bench_function("full_scan_prefix_automaton_both_hints_without_keys", |b| {
        b.iter(|| {
            assert_eq!(
                automaton_bench_without_keys(&dict, true, true),
                NUM_AUTOMATON_MATCHES
            )
        })
    });

    // A regex that cannot prune any block: every term goes through the automaton.
    let regex = Regex::new(".*ab.*").unwrap();
    let num_regex_matches = regex_bench(&dict, &regex);
    c.bench_function("full_scan_regex", |b| {
        b.iter(|| assert_eq!(regex_bench(&dict, &regex), num_regex_matches))
    });
    c.bench_function("full_scan_regex_without_keys", |b| {
        b.iter(|| assert_eq!(regex_bench_without_keys(&dict, &regex), num_regex_matches))
    });
}

struct PairCorpus {
    left: Vec<u8>,
    right: Vec<u8>,
    ranges: Vec<(usize, usize, usize, usize)>,
}

fn make_non_predictable_pairs(
    rng: &mut StdRng,
    count: usize,
    mut pick_lens_and_prefix: impl FnMut(&mut StdRng) -> (usize, usize, usize),
) -> PairCorpus {
    let mut left_buf = Vec::new();
    let mut right_buf = Vec::new();
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let (l_len, r_len, prefix_len) = pick_lens_and_prefix(rng);
        let l_start = left_buf.len();
        let r_start = right_buf.len();
        left_buf.resize(l_start + l_len, 0);
        right_buf.resize(r_start + r_len, 0);
        rng.fill(&mut left_buf[l_start..l_start + l_len]);
        rng.fill(&mut right_buf[r_start..r_start + r_len]);
        let shared = prefix_len.min(l_len).min(r_len);
        right_buf[r_start..r_start + shared].copy_from_slice(&left_buf[l_start..l_start + shared]);
        if shared < l_len
            && shared < r_len
            && left_buf[l_start + shared] == right_buf[r_start + shared]
        {
            right_buf[r_start + shared] ^= 0x01;
        }
        ranges.push((l_start, l_start + l_len, r_start, r_start + r_len));
    }
    PairCorpus {
        left: left_buf,
        right: right_buf,
        ranges,
    }
}

pub fn common_prefix_benchmark(c: &mut Criterion) {
    const NUM_PAIRS: usize = 50_000;
    let mut rng = StdRng::from_seed([29u8; 32]);

    // Short keys (0..=7 bytes) with random mismatch positions to verify the <8B path does not regress.
    let short = make_non_predictable_pairs(&mut rng, NUM_PAIRS, |r| {
        let l_len = r.random_range(0..=7);
        let r_len = r.random_range(0..=7);
        let min_len = l_len.min(r_len);
        let prefix = r.random_range(0..=min_len);
        (l_len, r_len, prefix)
    });
    c.bench_function("common_prefix_len_short_0_7", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for &(ls, le, rs, re) in &short.ranges {
                sum =
                    sum.wrapping_add(common_prefix_len(&short.left[ls..le], &short.right[rs..re]));
            }
            black_box(sum)
        })
    });

    // Mixed term lengths (2..=32 bytes, weighted toward 4..=16) with non-periodic mismatch positions.
    let mixed = make_non_predictable_pairs(&mut rng, NUM_PAIRS, |r| {
        let pick_len = |r: &mut StdRng| match r.random_range(0..100u32) {
            0..25 => r.random_range(2..=7),
            25..75 => r.random_range(8..=16),
            _ => r.random_range(17..=32),
        };
        let l_len = pick_len(r);
        let r_len = pick_len(r);
        let min_len = l_len.min(r_len);
        let prefix = r.random_range(0..=min_len);
        (l_len, r_len, prefix)
    });
    c.bench_function("common_prefix_len_mixed_terms", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for &(ls, le, rs, re) in &mixed.ranges {
                sum =
                    sum.wrapping_add(common_prefix_len(&mixed.left[ls..le], &mixed.right[rs..re]));
            }
            black_box(sum)
        })
    });

    // Long JSON path keys (24..=96 bytes) with long shared prefixes and occasional resets.
    let json = make_non_predictable_pairs(&mut rng, NUM_PAIRS, |r| {
        let l_len = r.random_range(24..=96);
        let r_len = r.random_range(24..=96);
        let min_len = l_len.min(r_len);
        let prefix = if r.random_range(0..20u32) == 0 {
            r.random_range(0..8)
        } else {
            r.random_range(8..=min_len)
        };
        (l_len, r_len, prefix)
    });
    c.bench_function("common_prefix_len_json_paths", |b| {
        b.iter(|| {
            let mut sum = 0usize;
            for &(ls, le, rs, re) in &json.ranges {
                sum = sum.wrapping_add(common_prefix_len(&json.left[ls..le], &json.right[rs..re]));
            }
            black_box(sum)
        })
    });

    // End-to-end SSTable build on realistic sorted JSON path terms.
    let prefixes = [
        "attributes\x01k8s\x01pod\x01labels\x01app\x01",
        "attributes\x01k8s\x01namespace\x01workload\x01",
        "attributes\x01http\x01request\x01headers\x01",
        "attributes\x01service\x01instance\x01region\x01",
        "resource\x01cloud\x01availability_zone\x01",
    ];
    let mut json_terms: Vec<Vec<u8>> = (0..20_000usize)
        .map(|i| {
            let p = prefixes[i % prefixes.len()];
            let shard = (i / prefixes.len()) % 64;
            format!("{p}cluster-{shard:02}\x01value-{i:08}").into_bytes()
        })
        .collect();
    json_terms.sort();
    c.bench_function("sstable_build_json_paths", |b| {
        b.iter(|| {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
            for (ord, term) in json_terms.iter().enumerate() {
                builder.insert(term, &(ord as u64)).unwrap();
            }
            black_box(builder.finish().unwrap().len())
        })
    });

    // End-to-end SSTable build on short keys (3..=12 bytes).
    let mut short_set = BTreeSet::new();
    while short_set.len() < 20_000 {
        short_set.insert(generate_key(&mut rng).into_bytes());
    }
    let short_terms: Vec<Vec<u8>> = short_set.into_iter().collect();
    c.bench_function("sstable_build_short_keys", |b| {
        b.iter(|| {
            let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();
            for (ord, term) in short_terms.iter().enumerate() {
                builder.insert(term, &(ord as u64)).unwrap();
            }
            black_box(builder.finish().unwrap().len())
        })
    });
}

criterion_group!(benches, criterion_benchmark, common_prefix_benchmark);
criterion_main!(benches);
