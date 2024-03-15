#![feature(test)]
extern crate test;

use std::sync::Arc;

use rand::prelude::*;
use tantivy_columnar::column_values::{serialize_and_load_u64_based_column_values, CodecType};
use tantivy_columnar::*;
use test::{black_box, Bencher};

struct Columns {
    pub optional: Column,
    pub full: Column,
    pub multi: Column,
}

fn get_test_columns() -> Columns {
    let data = generate_permutation();
    let mut dataframe_writer = ColumnarWriter::default();
    for (idx, val) in data.iter().enumerate() {
        dataframe_writer.record_numerical(idx as u32, "full_values", NumericalValue::U64(*val));
        if idx % 2 == 0 {
            dataframe_writer.record_numerical(
                idx as u32,
                "optional_values",
                NumericalValue::U64(*val),
            );
        }
        dataframe_writer.record_numerical(idx as u32, "multi_values", NumericalValue::U64(*val));
        dataframe_writer.record_numerical(idx as u32, "multi_values", NumericalValue::U64(*val));
    }
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer
        .serialize(data.len() as u32, None, &mut buffer)
        .unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();

    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("optional_values").unwrap();
    assert_eq!(cols.len(), 1);
    let optional = cols[0].open_u64_lenient().unwrap().unwrap();
    assert_eq!(optional.index.get_cardinality(), Cardinality::Optional);

    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("full_values").unwrap();
    assert_eq!(cols.len(), 1);
    let column_full = cols[0].open_u64_lenient().unwrap().unwrap();
    assert_eq!(column_full.index.get_cardinality(), Cardinality::Full);

    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("multi_values").unwrap();
    assert_eq!(cols.len(), 1);
    let multi = cols[0].open_u64_lenient().unwrap().unwrap();
    assert_eq!(multi.index.get_cardinality(), Cardinality::Multivalued);

    Columns {
        optional,
        full: column_full,
        multi,
    }
}

const NUM_VALUES: u64 = 100_000;
fn generate_permutation() -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..NUM_VALUES).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

pub fn serialize_and_load(column: &[u64], codec_type: CodecType) -> Arc<dyn ColumnValues<u64>> {
    serialize_and_load_u64_based_column_values(&column, &[codec_type])
}

fn run_bench_on_column_full_scan(b: &mut Bencher, column: Column) {
    let num_iter = black_box(NUM_VALUES);
    b.iter(|| {
        let mut sum = 0u64;
        for i in 0..num_iter as u32 {
            let val = column.first(i);
            sum += val.unwrap_or(0);
        }
        sum
    });
}
fn run_bench_on_column_block_fetch(b: &mut Bencher, column: Column) {
    let mut block: Vec<Option<u64>> = vec![None; 64];
    let fetch_docids = (0..64).collect::<Vec<_>>();
    b.iter(move || {
        column.first_vals(&fetch_docids, &mut block);
        block[0]
    });
}
fn run_bench_on_column_block_single_calls(b: &mut Bencher, column: Column) {
    let mut block: Vec<Option<u64>> = vec![None; 64];
    let fetch_docids = (0..64).collect::<Vec<_>>();
    b.iter(move || {
        for i in 0..fetch_docids.len() {
            block[i] = column.first(fetch_docids[i]);
        }
        block[0]
    });
}

/// Column first method
#[bench]
fn bench_get_first_on_full_column_full_scan(b: &mut Bencher) {
    let column = get_test_columns().full;
    run_bench_on_column_full_scan(b, column);
}

#[bench]
fn bench_get_first_on_optional_column_full_scan(b: &mut Bencher) {
    let column = get_test_columns().optional;
    run_bench_on_column_full_scan(b, column);
}

#[bench]
fn bench_get_first_on_multi_column_full_scan(b: &mut Bencher) {
    let column = get_test_columns().multi;
    run_bench_on_column_full_scan(b, column);
}

/// Block fetch column accessor
#[bench]
fn bench_get_block_first_on_optional_column(b: &mut Bencher) {
    let column = get_test_columns().optional;
    run_bench_on_column_block_fetch(b, column);
}

#[bench]
fn bench_get_block_first_on_multi_column(b: &mut Bencher) {
    let column = get_test_columns().multi;
    run_bench_on_column_block_fetch(b, column);
}

#[bench]
fn bench_get_block_first_on_full_column(b: &mut Bencher) {
    let column = get_test_columns().full;
    run_bench_on_column_block_fetch(b, column);
}

#[bench]
fn bench_get_block_first_on_optional_column_single_calls(b: &mut Bencher) {
    let column = get_test_columns().optional;
    run_bench_on_column_block_single_calls(b, column);
}

#[bench]
fn bench_get_block_first_on_multi_column_single_calls(b: &mut Bencher) {
    let column = get_test_columns().multi;
    run_bench_on_column_block_single_calls(b, column);
}

#[bench]
fn bench_get_block_first_on_full_column_single_calls(b: &mut Bencher) {
    let column = get_test_columns().full;
    run_bench_on_column_block_single_calls(b, column);
}
