use binggan::{BenchRunner, black_box};
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng, rng};
use tantivy_common::{
    BinarySerializable, BitSet, TinySet, VInt, VIntU128, read_u32_vint, serialize_vint_u32,
};

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

    let mut seeded = StdRng::from_seed([7u8; 32]);
    // Use 100k elements so random byte-length sequences overflow CPU branch predictors.
    const NUM_VALS: usize = 100_000;
    let small_u32: Vec<u32> = (0..NUM_VALS).map(|_| seeded.random_range(0..128)).collect();
    // Random 1-3 byte length distribution (70% 1B, 25% 2B, 5% 3B), matching typical
    // postings deltas, field lengths, and block sizes without periodic branch patterns.
    let mixed_u32: Vec<u32> = (0..NUM_VALS)
        .map(|_| match seeded.random_range(0..100u32) {
            0..70 => seeded.random_range(0..128),
            70..95 => seeded.random_range(128..(1 << 14)),
            _ => seeded.random_range((1 << 14)..(1 << 21)),
        })
        .collect();
    let encode_u32 = |vals: &[u32]| {
        let mut encoded = Vec::with_capacity(vals.len() * 2);
        let mut buf = [0u8; 8];
        for &val in vals {
            encoded.extend_from_slice(serialize_vint_u32(val, &mut buf));
        }
        encoded
    };
    let small_u32_encoded = encode_u32(&small_u32);
    let mixed_u32_encoded = encode_u32(&mixed_u32);

    runner.bench_function("bench_read_u32_vint_small", move |_| {
        let mut slice: &[u8] = &small_u32_encoded;
        let mut sum = 0u64;
        while !slice.is_empty() {
            sum = sum.wrapping_add(u64::from(read_u32_vint(&mut slice)));
        }
        black_box(sum);
    });

    runner.bench_function("bench_read_u32_vint_mixed", move |_| {
        let mut slice: &[u8] = &mixed_u32_encoded;
        let mut sum = 0u64;
        while !slice.is_empty() {
            sum = sum.wrapping_add(u64::from(read_u32_vint(&mut slice)));
        }
        black_box(sum);
    });

    // Random length distribution skewed toward 1-3 bytes with occasional larger offsets.
    let mixed_u64: Vec<u64> = (0..NUM_VALS)
        .map(|_| match seeded.random_range(0..100u32) {
            0..60 => seeded.random_range(0..128),
            60..85 => seeded.random_range(128..(1 << 14)),
            85..95 => seeded.random_range((1 << 14)..(1 << 21)),
            _ => seeded.random_range((1 << 21)..(1 << 35)),
        })
        .collect();
    let mut mixed_u64_encoded = Vec::with_capacity(mixed_u64.len() * 3);
    for &val in &mixed_u64 {
        VInt(val).serialize_into_vec(&mut mixed_u64_encoded);
    }

    runner.bench_function("bench_vint_u64_serialize_into", move |_| {
        let mut total = 0usize;
        let mut buf = [0u8; 10];
        for &val in &mixed_u64 {
            let len = VInt(val).serialize_into(&mut buf);
            total += len + usize::from(buf[0]);
        }
        black_box(total);
    });

    runner.bench_function("bench_vint_u64_deserialize", move |_| {
        let mut slice: &[u8] = &mixed_u64_encoded;
        let mut sum = 0u64;
        while !slice.is_empty() {
            sum = sum.wrapping_add(VInt::deserialize(&mut slice).unwrap().val());
        }
        black_box(sum);
    });

    let mixed_u128: Vec<u128> = (0..NUM_VALS)
        .map(|_| match seeded.random_range(0..100u32) {
            0..40 => seeded.random_range(0..(1 << 21)),
            40..75 => u128::from(seeded.random::<u64>()),
            _ => seeded.random::<u128>(),
        })
        .collect();
    let mut mixed_u128_encoded = Vec::with_capacity(mixed_u128.len() * 8);
    for &val in &mixed_u128 {
        VIntU128(val).serialize(&mut mixed_u128_encoded).unwrap();
    }

    runner.bench_function("bench_vint_u128_serialize", move |_| {
        let mut total = 0usize;
        let mut buf = [0u8; 19];
        for &val in &mixed_u128 {
            let mut cursor: &mut [u8] = &mut buf;
            VIntU128(val).serialize(&mut cursor).unwrap();
            let len = 19 - cursor.len();
            total += len + usize::from(buf[0]);
        }
        black_box(total);
    });

    runner.bench_function("bench_vint_u128_deserialize", move |_| {
        let mut slice: &[u8] = &mixed_u128_encoded;
        let mut sum = 0u128;
        while !slice.is_empty() {
            sum = sum.wrapping_add(VIntU128::deserialize(&mut slice).unwrap().0);
        }
        black_box(sum);
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
    bench_bitset();
}
