use std::collections::BTreeSet;
use std::hint::black_box;
use std::io;

use common::file_slice::FileSlice;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_fst::Automaton;
use tantivy_sstable::{Dictionary, MonotonicU64SSTable};

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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
