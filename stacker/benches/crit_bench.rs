#![allow(dead_code)]
extern crate criterion;

use criterion::*;
use rand::SeedableRng;
use tantivy_stacker::ArenaHashMap;

const ALICE: &str = include_str!("../../benches/alice.txt");

fn bench_hashmap_throughput(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Linear);

    let mut group = c.benchmark_group("CreateHashMap");
    group.plot_config(plot_config);

    let input_name = "alice";
    let input_bytes = ALICE.len() as u64;
    group.throughput(Throughput::Bytes(input_bytes));

    group.bench_with_input(
        BenchmarkId::new(input_name.to_string(), input_bytes),
        &ALICE,
        |b, i| b.iter(|| create_hash_map(i.split_whitespace().map(|el| el.as_bytes()))),
    );
    // numbers
    let input_bytes = 1_000_000 * 8 as u64;
    group.throughput(Throughput::Bytes(input_bytes));

    group.bench_with_input(
        BenchmarkId::new("numbers".to_string(), input_bytes),
        &(0..1_000_000u64),
        |b, i| b.iter(|| create_hash_map(i.clone().map(|el| el.to_le_bytes()))),
    );

    // numbers zipf
    use rand::distributions::Distribution;
    use rand::rngs::StdRng;
    let mut rng = StdRng::from_seed([3u8; 32]);
    let zipf = zipf::ZipfDistribution::new(10_000, 1.03).unwrap();

    let input_bytes = 1_000_000 * 8 as u64;
    group.throughput(Throughput::Bytes(input_bytes));

    group.bench_with_input(
        BenchmarkId::new("numbers_zipf".to_string(), input_bytes),
        &(0..1_000_000u64),
        |b, i| b.iter(|| create_hash_map(i.clone().map(|_el| zipf.sample(&mut rng).to_le_bytes()))),
    );

    group.finish();
}

fn create_hash_map<'a, T: AsRef<[u8]>>(terms: impl Iterator<Item = T>) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(4);
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

criterion_group!(block_benches, bench_hashmap_throughput,);
criterion_main!(block_benches);
