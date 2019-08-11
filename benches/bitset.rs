use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::{Bernoulli, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy::forbench::bitset::{BitSet, TinySet};
use tantivy::query::BitSetDocSet;
use tantivy::DocSet;

fn sample_with_seed(n: u32, ratio: f64, seed_val: u8) -> Vec<u32> {
    StdRng::from_seed([seed_val; 32])
        .sample_iter(&Bernoulli::new(ratio).unwrap())
        .take(n as usize)
        .enumerate()
        .filter_map(|(val, keep)| if keep { Some(val as u32) } else { None })
        .collect()
}

fn generate_nonunique_unsorted(max_value: u32, n_elems: usize) -> Vec<u32> {
    let seed: [u8; 32] = [1; 32];
    StdRng::from_seed(seed)
        .sample_iter(&Uniform::new(0u32, max_value))
        .take(n_elems)
        .collect::<Vec<u32>>()
}

fn bench_tinyset_pop(criterion: &mut Criterion) {
    criterion.bench_function("pop_lowest", |b| {
        b.iter(|| {
            let mut tinyset = TinySet::singleton(criterion::black_box(31u32));
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
        })
    });
}

fn bench_bitset_insert(criterion: &mut Criterion) {
    criterion.bench_function_over_inputs(
        "bitset_insert",
        |bench, (max_value, n_elems)| {
            let els = generate_nonunique_unsorted(*max_value, *n_elems);
            bench.iter(move || {
                let mut bitset = BitSet::with_max_value(1_000_000);
                for el in els.iter().cloned() {
                    bitset.insert(el);
                }
            });
        },
        vec![(1_000_000u32, 10_000)],
    );
}

fn bench_bitsetdocset_iterate(b: &mut test::Bencher) {
    let mut bitset = BitSet::with_max_value(1_000_000);
    for el in sample_with_seed(1_000_000u32, 0.01, 0u8) {
        bitset.insert(el);
    }
    b.iter(|| {
        let mut docset = BitSetDocSet::from(bitset.clone());
        while docset.advance() {}
    });
}

criterion_group!(
    benches,
    bench_tinyset_pop,
    bench_bitset_insert,
    bench_bitsetdocset_iterate
);
criterion_main!(benches);
