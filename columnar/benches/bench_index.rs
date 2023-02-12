use std::ops::Range;

use criterion::*;
use rand::prelude::*;
use tantivy_columnar::column_index::MultiValueIndex;
use tantivy_columnar::RowId;

const WINDOW: usize = 40;

fn bench_multi_value_index_util(
    len_range: Range<u32>,
    num_rows: RowId,
    select_value_ratio: f64,
    b: &mut criterion::Bencher,
) {
    let mut start_index: Vec<RowId> = vec![0u32];
    let mut cursor: u32 = 0u32;
    let mut rng = StdRng::from_seed([16u8; 32]);
    for i in 0..num_rows {
        let num_vals = rng.gen_range(len_range.clone());
        cursor += num_vals;
        start_index.push(cursor);
    }
    let select_rows: Vec<RowId> = (0u32..cursor)
        .filter(|i| rng.gen_bool(select_value_ratio))
        .collect();
    let mv_index = MultiValueIndex::for_test(&start_index);

    // mv_index.select_batch_in_place(0, &mut select_rows[..]);
    let mut buffer = Vec::new();
    b.iter(|| {
        let mut start_row = 0u32;
        let mut len = 0;
        for chunk in select_rows.chunks(WINDOW) {
            buffer.clear();
            buffer.extend_from_slice(chunk);
            mv_index.select_batch_in_place(start_row, &mut buffer);
            start_row = buffer.last().copied().unwrap();
            len += buffer.len()
        }
        assert_eq!(len, 4303);
        len
    });
}

fn bench_multi_value_index_util2(
    len_range: Range<u32>,
    num_rows: RowId,
    select_value_ratio: f64,
    b: &mut criterion::Bencher,
) {
    let mut start_index: Vec<RowId> = vec![0u32];
    let mut cursor: u32 = 0u32;
    let mut rng = StdRng::from_seed([16u8; 32]);
    for i in 0..num_rows {
        let num_vals = rng.gen_range(len_range.clone());
        cursor += num_vals;
        start_index.push(cursor);
    }
    let select_rows: Vec<RowId> = (0u32..cursor)
        .filter(|i| rng.gen_bool(select_value_ratio))
        .collect();
    let mv_index = MultiValueIndex::for_test(&start_index);

    // mv_index.select_batch_in_place(0, &mut select_rows[..]);
    let mut buffer = Vec::new();
    b.iter(|| {
        let mut mv_index_cursor = mv_index.select_cursor();
        let mut len = 0;
        for chunk in select_rows.chunks(WINDOW) {
            buffer.clear();
            buffer.extend_from_slice(chunk);
            mv_index_cursor.select_batch_in_place(&mut buffer);
            len += buffer.len();
        }
        assert_eq!(len, 4303);
        len
    });
}

fn select_benchmark(c: &mut criterion::Criterion) {
    c.bench_function("bench_multi_value_index_10_100", |b| {
        bench_multi_value_index_util(0..10, 100_000, 0.01f64, b)
    });
    c.bench_function("bench_multi_value_cursor_index_10_100", |b| {
        bench_multi_value_index_util2(0..10, 100_000, 0.01f64, b)
    });
}

criterion_group!(benches, select_benchmark);
criterion_main!(benches);
