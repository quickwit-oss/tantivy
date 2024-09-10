use binggan::{black_box, BenchRunner};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use tantivy_common::{serialize_vint_u32, BitSet, TinySet};

fn bench_vint() {
    let mut runner = BenchRunner::new();

    let vals: Vec<u32> = (0..20_000).collect();
    runner.bench_function("bench_vint", move |_| {
        let mut out = 0u64;
        for val in vals.iter().cloned() {
            let mut buf = [0u8; 8];
            serialize_vint_u32(val, &mut buf);
            out += u64::from(buf[0]);
        }
        black_box(out);
        None
    });

    let vals: Vec<u32> = (0..20_000).choose_multiple(&mut thread_rng(), 100_000);
    runner.bench_function("bench_vint_rand", move |_| {
        let mut out = 0u64;
        for val in vals.iter().cloned() {
            let mut buf = [0u8; 8];
            serialize_vint_u32(val, &mut buf);
            out += u64::from(buf[0]);
        }
        black_box(out);
        None
    });
}

fn bench_bitset() {
    let mut runner = BenchRunner::new();

    runner.bench_function("bench_tinyset_pop", move |_| {
        let mut tinyset = TinySet::singleton(black_box(31u32));
        tinyset.pop_lowest();
        tinyset.pop_lowest();
        tinyset.pop_lowest();
        tinyset.pop_lowest();
        tinyset.pop_lowest();
        tinyset.pop_lowest();
        black_box(tinyset);
        None
    });

    let tiny_set = TinySet::empty().insert(10u32).insert(14u32).insert(21u32);
    runner.bench_function("bench_tinyset_sum", move |_| {
        assert_eq!(black_box(tiny_set).into_iter().sum::<u32>(), 45u32);
        None
    });

    let v = [10u32, 14u32, 21u32];
    runner.bench_function("bench_tinyarr_sum", move |_| {
        black_box(v.iter().cloned().sum::<u32>());
        None
    });

    runner.bench_function("bench_bitset_initialize", move |_| {
        black_box(BitSet::with_max_value(1_000_000));
        None
    });
}

fn main() {
    bench_vint();
    bench_bitset();
}
