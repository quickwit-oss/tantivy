use std::sync::Arc;

use common::file_slice::FileSlice;
use common::OwnedBytes;
use criterion::{criterion_group, criterion_main, Criterion};
use tantivy_sstable::{self, Dictionary, MonotonicU64SSTable};

fn make_test_sstable(suffix: &str) -> FileSlice {
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

    // 125 mio elements
    for elem in 0..125_000_000 {
        let key = format!("prefix.{elem:07X}{suffix}").into_bytes();
        builder.insert(&key, &elem).unwrap();
    }

    let table = builder.finish().unwrap();
    let table = Arc::new(OwnedBytes::new(table));
    let slice = common::file_slice::FileSlice::new(table.clone());

    slice
}

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let slice = make_test_sstable(".suffix");
        let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
        c.bench_function("ord_to_term_suffix", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("open_and_ord_to_term_suffix", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
    }
    {
        let slice = make_test_sstable("");
        let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
        c.bench_function("ord_to_term", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("open_and_ord_to_term", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
