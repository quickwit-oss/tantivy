use binggan::{BenchRunner, black_box};
use rand::rng;
use rand::seq::IteratorRandom;
use tantivy_common::{BinarySerializable, BitSet, TinySet, VInt, serialize_vint_u32};

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
    });

    let vals: Vec<u32> = (0..20_000).choose_multiple(&mut rng(), 100_000);
    runner.bench_function("bench_vint_rand", move |_| {
        let mut out = 0u64;
        for val in vals.iter().cloned() {
            let mut buf = [0u8; 8];
            serialize_vint_u32(val, &mut buf);
            out += u64::from(buf[0]);
        }
        black_box(out);
    });
}

fn bench_vint_serialize_to_writer() {
    // "one byte": lengths and counts written while serializing documents, almost always below
    // 128. "mostly one byte": the same with a multi-byte value every 16 values. "all >= 128":
    // only multi-byte values.
    let one_byte: Vec<u64> = (0..100_000).map(|i| i % 100).collect();
    let mostly_one_byte: Vec<u64> = (0..100_000u64)
        .map(|i| if i % 16 == 0 { i * 1_000 } else { i % 100 })
        .collect();
    let multi_byte: Vec<u64> = (0..100_000u64).map(|i| 128 + i * 37 % 100_000).collect();
    let mut runner = BenchRunner::new();
    let mut group = runner.new_group();
    group.set_name("VInt::serialize into a Vec<u8>");
    for (name, vals) in [
        ("one byte", &one_byte),
        ("mostly one byte", &mostly_one_byte),
        ("all >= 128", &multi_byte),
    ] {
        group.register_with_input(name, vals, move |vals| {
            let mut buffer = Vec::with_capacity(vals.len() * 10);
            for &val in vals {
                VInt(val).serialize(&mut buffer).unwrap();
            }
            black_box(buffer);
        });
    }
    group.run();
}

fn bench_string_serialize_to_writer() {
    // Long messages: a 2-byte VInt length prefix followed by the text.
    let long_strings: Vec<String> = (0..10_000usize)
        .map(|i| "x".repeat(200 + (i * 37) % 1_800))
        .collect();
    let mut runner = BenchRunner::new();
    let mut group = runner.new_group();
    group.set_name("String::serialize into a Vec<u8>");
    group.register_with_input("200..2000 bytes", &long_strings, move |strs| {
        let mut buffer = Vec::with_capacity(12_000_000);
        for s in strs {
            s.serialize(&mut buffer).unwrap();
        }
        black_box(buffer);
    });
    group.run();
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
    });

    let tiny_set = TinySet::empty().insert(10u32).insert(14u32).insert(21u32);
    runner.bench_function("bench_tinyset_sum", move |_| {
        assert_eq!(black_box(tiny_set).into_iter().sum::<u32>(), 45u32);
    });

    let v = [10u32, 14u32, 21u32];
    runner.bench_function("bench_tinyarr_sum", move |_| {
        black_box(v.iter().cloned().sum::<u32>());
    });

    runner.bench_function("bench_bitset_initialize", move |_| {
        black_box(BitSet::with_max_value(1_000_000));
    });
}

fn main() {
    bench_vint();
    bench_vint_serialize_to_writer();
    bench_string_serialize_to_writer();
    bench_bitset();
}
