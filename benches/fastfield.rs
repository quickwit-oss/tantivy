use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use criterion::ParameterizedBenchmark;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tantivy::schema::{Schema, FAST};
use tantivy::{doc, DocId, Index};

const NUM_LOOKUPS: usize = 1_000;

fn generate_permutation(stride: usize, bit_width: u8) -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..(NUM_LOOKUPS * stride) as u64).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation.push(1u64 << (bit_width as u64)); //< just to force the bit_width
    permutation
}

fn bench_linear_lookup(c: &mut Criterion) {
    c.bench(
        "lookup_stride",
        ParameterizedBenchmark::new(
            "baseline_vec",
            |bench, (stride, num_bits)| {
                let arr = generate_permutation(*stride, *num_bits);
                bench.iter(move || {
                    let mut a = 0u64;
                    for i in (0..NUM_LOOKUPS / stride).map(|v| v * 7) {
                        a ^= arr[i as usize];
                    }
                    a
                })
            },
            vec![(7, 1), (7, 5), (7, 20)],
        )
        .with_function("fastfield", |bench, (stride, num_bits)| {
            let mut schema_builder = Schema::builder();
            let val_field = schema_builder.add_u64_field("val", FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_with_num_threads(1, 80_000_000).unwrap();
            for el in generate_permutation(*stride, *num_bits) {
                index_writer.add_document(doc!(val_field=>el));
            }
            index_writer.commit().unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            let segment_reader = searcher.segment_reader(0u32);
            let fast_field_reader = segment_reader.fast_fields().u64(val_field).unwrap();
            bench.iter(move || {
                let mut a = 0u64;
                for i in (0..NUM_LOOKUPS / stride).map(|v| v * 7) {
                    a ^= fast_field_reader.get(i as DocId);
                }
                a
            })
        }),
    );
}

fn bench_jumpy_lookup(c: &mut Criterion) {
    c.bench(
        "lookup_jumpy",
        ParameterizedBenchmark::new(
            "baseline_vec",
            |bench, (stride, num_bits)| {
                let arr = generate_permutation(*stride, *num_bits);
                bench.iter(move || {
                    let mut a = 0u64;
                    for _ in 0..NUM_LOOKUPS {
                        a = arr[a as usize];
                    }
                    a
                })
            },
            vec![(7, 1), (7, 5), (7, 20)],
        )
        .with_function("fastfield", |bench, (stride, num_bits)| {
            let mut schema_builder = Schema::builder();
            let val_field = schema_builder.add_u64_field("val", FAST);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_with_num_threads(1, 80_000_000).unwrap();
            for el in generate_permutation(*stride, *num_bits) {
                index_writer.add_document(doc!(val_field=>el));
            }
            index_writer.commit().unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();
            let segment_reader = searcher.segment_reader(0u32);
            let fast_field_reader = segment_reader.fast_fields().u64(val_field).unwrap();
            bench.iter(move || {
                let mut a = 0u64;
                for _ in 0..NUM_LOOKUPS {
                    a = fast_field_reader.get(a as DocId);
                }
                a
            })
        }),
    );
}

criterion_group!(benches, bench_linear_lookup, bench_jumpy_lookup);
criterion_main!(benches);
