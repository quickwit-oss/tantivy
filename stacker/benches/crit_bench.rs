#![allow(dead_code)]
extern crate criterion;

use tantivy_stacker::ArenaHashMap;

use criterion::*;

const ALICE: &str = include_str!("../../benches/alice.txt");
const HDFS: &str = include_str!("../../benches/hdfs.json");

//const ALL: &[(&str, &str)] = &[("alice", ALICE), ("hdfs", HDFS)];
const ALL: &[(&str, &str)] = &[("alice", ALICE)];

fn bench_hashmap_throughput(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Linear);

    let mut group = c.benchmark_group("CreateHashMap");
    group.plot_config(plot_config);

    for (input_name, input) in ALL {
        let input_bytes = input.len() as u64;
        group.throughput(Throughput::Bytes(input_bytes));

        group.bench_with_input(
            BenchmarkId::new(input_name.to_string(), input_bytes),
            &input,
            |b, i| b.iter(|| create_hash_map(i)),
        );
    }
    group.finish();
}

fn create_hash_map(terms: &str) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(4);
    for term in terms.split_whitespace() {
        map.mutate_or_create(term.as_bytes(), |val| {
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
