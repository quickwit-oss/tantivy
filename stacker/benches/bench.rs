use binggan::plugins::PeakMemAllocPlugin;
use binggan::{BenchRunner, INSTRUMENTED_SYSTEM, PeakMemAlloc, black_box};
use rand::SeedableRng;
use rustc_hash::FxHashMap;
use tantivy_stacker::{ArenaHashMap, ExpUnrolledLinkedList, MemoryArena};

const ALICE: &str = include_str!("../../benches/alice.txt");

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn bench_vint() {
    let mut runner = BenchRunner::new();
    // Set the peak mem allocator. This will enable peak memory reporting.
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    {
        let input_bytes = ALICE.len();

        let alice_terms_as_bytes: Vec<&[u8]> = ALICE
            .split_ascii_whitespace()
            .map(|el| el.as_bytes())
            .collect();

        let alice_terms_as_bytes_with_docid: Vec<(u32, &[u8])> = ALICE
            .split_ascii_whitespace()
            .map(|el| el.as_bytes())
            .enumerate()
            .map(|(docid, el)| (docid as u32, el))
            .collect();

        // Alice benchmark
        let mut group = runner.new_group();
        group.set_name(format!("alice (num terms: {})", ALICE.len()));
        group.set_input_size(input_bytes);
        group.register_with_input("hashmap", &alice_terms_as_bytes, move |data| {
            black_box(create_hash_map(data.iter()));
        });
        group.register_with_input(
            "hasmap with postings",
            &alice_terms_as_bytes_with_docid,
            move |data| {
                black_box(create_hash_map_with_expull(data.iter().cloned()));
            },
        );
        group.register_with_input(
            "fxhashmap ref postings",
            &alice_terms_as_bytes,
            move |data| {
                black_box(create_fx_hash_ref_map_with_expull(data.iter().cloned()));
            },
        );
        group.register_with_input(
            "fxhasmap owned postings",
            &alice_terms_as_bytes,
            move |data| {
                black_box(create_fx_hash_owned_map_with_expull(data.iter().cloned()));
            },
        );
        group.run();
    }

    {
        for (num_numbers, num_numbers_label) in [
            (100_000u64, "100k"),
            (1_000_000, "1mio"),
            (2_000_000, "2mio"),
            (5_000_000, "5mio"),
        ] {
            // benchmark unique numbers
            {
                let numbers: Vec<[u8; 8]> = (0..num_numbers).map(|el| el.to_le_bytes()).collect();
                let numbers_with_doc: Vec<_> = numbers
                    .iter()
                    .enumerate()
                    .map(|(docid, el)| (docid as u32, el))
                    .collect();

                let input_bytes = numbers.len() * 8;
                let mut group = runner.new_group();
                group.set_name(format!("numbers unique {}", num_numbers_label));
                group.set_input_size(input_bytes);
                group.register_with_input("only hashmap", &numbers, move |data| {
                    black_box(create_hash_map(data.iter()));
                });
                group.register_with_input("hasmap with postings", &numbers_with_doc, move |data| {
                    black_box(create_hash_map_with_expull(data.iter().cloned()));
                });
                group.run();
            }
            // benchmark zipfs distribution numbers
            {
                use rand::distr::Distribution;
                use rand::rngs::StdRng;
                let mut rng = StdRng::from_seed([3u8; 32]);
                let zipf = rand_distr::Zipf::new(10_000.0f64, 1.03).unwrap();
                let numbers: Vec<[u8; 8]> = (0..num_numbers)
                    .map(|_| zipf.sample(&mut rng).to_le_bytes())
                    .collect();
                let numbers_with_doc: Vec<_> = numbers
                    .iter()
                    .enumerate()
                    .map(|(docid, el)| (docid as u32, el))
                    .collect();

                let input_bytes = numbers.len() * 8;
                let mut group = runner.new_group();
                group.set_name(format!("zipfs numbers {}", num_numbers_label));
                group.set_input_size(input_bytes);
                group.register_with_input("hashmap", &numbers, move |data| {
                    black_box(create_hash_map(data.iter()));
                });
                group.register_with_input("hasmap with postings", &numbers_with_doc, move |data| {
                    black_box(create_hash_map_with_expull(data.iter().cloned()));
                });
                group.run();
            }
        }
    }
}

#[cfg(not(feature = "compare_hash_only"))]
fn bench_fastcmp() {
    use rand::Rng;
    use rand::rngs::StdRng;
    use tantivy_stacker::fast_short_slice_compare;

    let mut runner = BenchRunner::new();

    // Build 100k pairs in contiguous backing buffers so slice lengths and match/mismatch outcomes
    // are non-periodic and overflow CPU branch predictors.
    fn make_pairs(
        rng: &mut StdRng,
        count: usize,
        mismatch_pct: u32,
        mut pick_len: impl FnMut(&mut StdRng) -> usize,
    ) -> (Vec<u8>, Vec<u8>, Vec<(usize, usize)>) {
        let mut left_buf = Vec::new();
        let mut right_buf = Vec::new();
        let mut ranges = Vec::with_capacity(count);
        for _ in 0..count {
            let len = pick_len(rng);
            let start = left_buf.len();
            left_buf.resize(start + len, 0);
            rng.fill(&mut left_buf[start..start + len]);
            right_buf.extend_from_slice(&left_buf[start..start + len]);
            if len > 0 && rng.random_range(0..100u32) < mismatch_pct {
                let diff_pos = rng.random_range(0..len);
                right_buf[start + diff_pos] ^= 0x01;
            }
            ranges.push((start, start + len));
        }
        (left_buf, right_buf, ranges)
    }

    const NUM_PAIRS: usize = 100_000;
    let mut rng = StdRng::from_seed([11u8; 32]);

    // Short token lengths (1..=16B, skewed like English words), 95% equal / 5% random mismatch.
    let (short_l, short_r, short_ranges) = make_pairs(&mut rng, NUM_PAIRS, 5, |r| {
        match r.random_range(0..100u32) {
            0..30 => r.random_range(1..4),
            30..75 => r.random_range(4..8),
            _ => r.random_range(8..=16),
        }
    });

    // Mixed log/JSON key lengths (1..=96B), 95% equal / 5% random mismatch.
    let (mixed_l, mixed_r, mixed_ranges) = make_pairs(&mut rng, NUM_PAIRS, 5, |r| {
        match r.random_range(0..100u32) {
            0..25 => r.random_range(1..4),
            25..50 => r.random_range(4..8),
            50..75 => r.random_range(8..=16),
            75..90 => r.random_range(17..=32),
            _ => r.random_range(33..=96),
        }
    });

    // Long JSON path keys (33..=128B), 95% equal / 5% random mismatch.
    let (long_l, long_r, long_ranges) =
        make_pairs(&mut rng, NUM_PAIRS, 5, |r| r.random_range(33..=128));

    let mut group = runner.new_group();
    group.set_name("fastcmp");
    group.register("fastcmp_short_1_16", move |_| {
        let mut eq = 0usize;
        for &(start, end) in &short_ranges {
            eq += usize::from(fast_short_slice_compare(
                &short_l[start..end],
                &short_r[start..end],
            ));
        }
        black_box(eq);
    });
    group.register("fastcmp_mixed_1_96", move |_| {
        let mut eq = 0usize;
        for &(start, end) in &mixed_ranges {
            eq += usize::from(fast_short_slice_compare(
                &mixed_l[start..end],
                &mixed_r[start..end],
            ));
        }
        black_box(eq);
    });
    group.register("fastcmp_long_33_128", move |_| {
        let mut eq = 0usize;
        for &(start, end) in &long_ranges {
            eq += usize::from(fast_short_slice_compare(
                &long_l[start..end],
                &long_r[start..end],
            ));
        }
        black_box(eq);
    });
    group.run();
}

fn main() {
    #[cfg(not(feature = "compare_hash_only"))]
    bench_fastcmp();
    bench_vint();
}

const HASHMAP_CAPACITY: usize = 1 << 15;

/// Only records the doc ids
#[derive(Clone, Default, Copy)]
pub struct DocIdRecorder {
    stack: ExpUnrolledLinkedList,
}
impl DocIdRecorder {
    fn new_doc(&mut self, doc: u32, arena: &mut MemoryArena) {
        self.stack.writer(arena).write_u32_vint(doc);
    }
}

fn create_hash_map<T: AsRef<[u8]>>(terms: impl Iterator<Item = T>) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(HASHMAP_CAPACITY);
    for term in terms {
        map.mutate_or_create(term.as_ref(), |val| {
            if let Some(mut val) = val {
                val += 1;
                val
            } else {
                1u64
            }
        });
    }

    map
}

fn create_hash_map_with_expull<T: AsRef<[u8]>>(
    terms: impl Iterator<Item = (u32, T)>,
) -> ArenaHashMap {
    let mut memory_arena = MemoryArena::default();
    let mut map = ArenaHashMap::with_capacity(HASHMAP_CAPACITY);
    for (i, term) in terms {
        map.mutate_or_create(term.as_ref(), |val: Option<DocIdRecorder>| {
            if let Some(mut rec) = val {
                rec.new_doc(i, &mut memory_arena);
                rec
            } else {
                DocIdRecorder::default()
            }
        });
    }

    map
}

fn create_fx_hash_ref_map_with_expull(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<&'static [u8], Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_CAPACITY, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}

fn create_fx_hash_owned_map_with_expull(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<Vec<u8>, Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_CAPACITY, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref().to_vec())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}
