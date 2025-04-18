use std::sync::Arc;

use common::OwnedBytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test::{self, Bencher};

use super::*;
use crate::column_values::u64_based::*;

fn get_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(2u64);
    let mut data: Vec<_> = (100..55000_u64)
        .map(|num| num + rng.r#gen::<u8>() as u64)
        .collect();
    data.push(99_000);
    data.insert(1000, 2000);
    data.insert(2000, 100);
    data.insert(3000, 4100);
    data.insert(4000, 100);
    data.insert(5000, 800);
    data
}

fn compute_stats(vals: impl Iterator<Item = u64>) -> ColumnStats {
    let mut stats_collector = StatsCollector::default();
    for val in vals {
        stats_collector.collect(val);
    }
    stats_collector.stats()
}

#[inline(never)]
fn value_iter() -> impl Iterator<Item = u64> {
    0..20_000
}

fn get_reader_for_bench<Codec: ColumnCodec>(data: &[u64]) -> Codec::ColumnValues {
    let mut bytes = Vec::new();
    let stats = compute_stats(data.iter().cloned());
    let mut codec_serializer = Codec::estimator();
    for val in data {
        codec_serializer.collect(*val);
    }
    codec_serializer
        .serialize(&stats, Box::new(data.iter().copied()).as_mut(), &mut bytes)
        .unwrap();

    Codec::load(OwnedBytes::new(bytes)).unwrap()
}

fn bench_get<Codec: ColumnCodec>(b: &mut Bencher, data: &[u64]) {
    let col = get_reader_for_bench::<Codec>(data);
    b.iter(|| {
        let mut sum = 0u64;
        for pos in value_iter() {
            let val = col.get_val(pos as u32);
            sum = sum.wrapping_add(val);
        }
        sum
    });
}

#[inline(never)]
fn bench_get_dynamic_helper(b: &mut Bencher, col: Arc<dyn ColumnValues>) {
    b.iter(|| {
        let mut sum = 0u64;
        for pos in value_iter() {
            let val = col.get_val(pos as u32);
            sum = sum.wrapping_add(val);
        }
        sum
    });
}

fn bench_get_dynamic<Codec: ColumnCodec>(b: &mut Bencher, data: &[u64]) {
    let col = Arc::new(get_reader_for_bench::<Codec>(data));
    bench_get_dynamic_helper(b, col);
}
fn bench_create<Codec: ColumnCodec>(b: &mut Bencher, data: &[u64]) {
    let stats = compute_stats(data.iter().cloned());

    let mut bytes = Vec::new();
    b.iter(|| {
        bytes.clear();
        let mut codec_serializer = Codec::estimator();
        for val in data.iter().take(1024) {
            codec_serializer.collect(*val);
        }

        codec_serializer.serialize(&stats, Box::new(data.iter().copied()).as_mut(), &mut bytes)
    });
}

#[bench]
fn bench_fastfield_bitpack_create(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_create::<BitpackedCodec>(b, &data);
}
#[bench]
fn bench_fastfield_linearinterpol_create(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_create::<LinearCodec>(b, &data);
}
#[bench]
fn bench_fastfield_multilinearinterpol_create(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_create::<BlockwiseLinearCodec>(b, &data);
}
#[bench]
fn bench_fastfield_bitpack_get(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get::<BitpackedCodec>(b, &data);
}
#[bench]
fn bench_fastfield_bitpack_get_dynamic(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get_dynamic::<BitpackedCodec>(b, &data);
}
#[bench]
fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get::<LinearCodec>(b, &data);
}
#[bench]
fn bench_fastfield_linearinterpol_get_dynamic(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get_dynamic::<LinearCodec>(b, &data);
}
#[bench]
fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get::<BlockwiseLinearCodec>(b, &data);
}
#[bench]
fn bench_fastfield_multilinearinterpol_get_dynamic(b: &mut Bencher) {
    let data: Vec<_> = get_data();
    bench_get_dynamic::<BlockwiseLinearCodec>(b, &data);
}
