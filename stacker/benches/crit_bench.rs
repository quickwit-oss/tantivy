#![allow(dead_code)]
extern crate criterion;

use criterion::*;
use rand::SeedableRng;
use rustc_hash::FxHashMap;
use tantivy_stacker::{ArenaHashMap, ExpUnrolledLinkedList, MemoryArena};

const ALICE: &str = include_str!("../../benches/alice.txt");

fn bench_hashmap_throughput(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Linear);

    let mut group = c.benchmark_group("CreateHashMap");
    group.plot_config(plot_config);

    let input_bytes = ALICE.len() as u64;

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

    group.throughput(Throughput::Bytes(input_bytes));

    group.bench_with_input(
        BenchmarkId::new("alice".to_string(), input_bytes),
        &alice_terms_as_bytes,
        |b, i| b.iter(|| create_hash_map(i.iter())),
    );
    group.bench_with_input(
        BenchmarkId::new("alice_expull".to_string(), input_bytes),
        &alice_terms_as_bytes_with_docid,
        |b, i| b.iter(|| create_hash_map_with_expull(i.iter().cloned())),
    );

    group.bench_with_input(
        BenchmarkId::new("alice_fx_hashmap_ref_expull".to_string(), input_bytes),
        &alice_terms_as_bytes,
        |b, i| b.iter(|| create_fx_hash_ref_map_with_expull(i.iter().cloned())),
    );

    group.bench_with_input(
        BenchmarkId::new("alice_fx_hashmap_owned_expull".to_string(), input_bytes),
        &alice_terms_as_bytes,
        |b, i| b.iter(|| create_fx_hash_owned_map_with_expull(i.iter().cloned())),
    );

    // numbers
    let input_bytes = 1_000_000 * 8 as u64;
    group.throughput(Throughput::Bytes(input_bytes));
    let numbers: Vec<[u8; 8]> = (0..1_000_000u64).map(|el| el.to_le_bytes()).collect();

    group.bench_with_input(
        BenchmarkId::new("numbers".to_string(), input_bytes),
        &numbers,
        |b, i| b.iter(|| create_hash_map(i.iter().cloned())),
    );

    let numbers_with_doc: Vec<_> = numbers
        .iter()
        .enumerate()
        .map(|(docid, el)| (docid as u32, el))
        .collect();

    group.bench_with_input(
        BenchmarkId::new("ids_expull".to_string(), input_bytes),
        &numbers_with_doc,
        |b, i| b.iter(|| create_hash_map_with_expull(i.iter().cloned())),
    );

    // numbers zipf
    use rand::distributions::Distribution;
    use rand::rngs::StdRng;
    let mut rng = StdRng::from_seed([3u8; 32]);
    let zipf = zipf::ZipfDistribution::new(10_000, 1.03).unwrap();

    let input_bytes = 1_000_000 * 8 as u64;
    group.throughput(Throughput::Bytes(input_bytes));
    let zipf_numbers: Vec<[u8; 8]> = (0..1_000_000u64)
        .map(|_| zipf.sample(&mut rng).to_le_bytes())
        .collect();

    group.bench_with_input(
        BenchmarkId::new("numbers_zipf".to_string(), input_bytes),
        &zipf_numbers,
        |b, i| b.iter(|| create_hash_map(i.iter().cloned())),
    );

    group.finish();
}

const HASHMAP_SIZE: usize = 1 << 15;

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

fn create_hash_map<'a, T: AsRef<[u8]>>(terms: impl Iterator<Item = T>) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(HASHMAP_SIZE);
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

fn create_hash_map_with_expull<'a, T: AsRef<[u8]>>(
    terms: impl Iterator<Item = (u32, T)>,
) -> ArenaHashMap {
    let mut memory_arena = MemoryArena::default();
    let mut map = ArenaHashMap::with_capacity(HASHMAP_SIZE);
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

fn create_fx_hash_ref_map_with_expull<'a>(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<&'static [u8], Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_SIZE, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}

fn create_fx_hash_owned_map_with_expull<'a>(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<Vec<u8>, Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_SIZE, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref().to_vec())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}

criterion_group!(block_benches, bench_hashmap_throughput,);
criterion_main!(block_benches);
