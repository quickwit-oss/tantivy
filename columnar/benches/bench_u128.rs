#![feature(test)]

use std::ops::RangeInclusive;
use std::sync::Arc;

use common::OwnedBytes;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{random, Rng, SeedableRng};
use tantivy_columnar::ColumnValues;
use test::Bencher;
extern crate test;

// TODO does this make sense for IPv6 ?
fn generate_random() -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..100_000u64)
        .map(|el| el + random::<u16>() as u64)
        .collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

fn get_u128_column_random() -> Arc<dyn ColumnValues<u128>> {
    let permutation = generate_random();
    let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
    get_u128_column_from_data(&permutation)
}

fn get_u128_column_from_data(data: &[u128]) -> Arc<dyn ColumnValues<u128>> {
    let mut out = vec![];
    tantivy_columnar::column_values::serialize_column_values_u128(&data, &mut out).unwrap();
    let out = OwnedBytes::new(out);
    tantivy_columnar::column_values::open_u128_mapped::<u128>(out).unwrap()
}

const FIFTY_PERCENT_RANGE: RangeInclusive<u64> = 1..=50;
const SINGLE_ITEM: u64 = 90;
const SINGLE_ITEM_RANGE: RangeInclusive<u64> = 90..=90;

fn get_data_50percent_item() -> Vec<u128> {
    let mut rng = StdRng::from_seed([1u8; 32]);

    let mut data = vec![];
    for _ in 0..300_000 {
        let val = rng.gen_range(1..=100);
        data.push(val);
    }
    data.push(SINGLE_ITEM);
    data.shuffle(&mut rng);
    let data = data.iter().map(|el| *el as u128).collect::<Vec<_>>();
    data
}

#[bench]
fn bench_intfastfield_getrange_u128_50percent_hit(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let column = get_u128_column_from_data(&data);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(
            *FIFTY_PERCENT_RANGE.start() as u128..=*FIFTY_PERCENT_RANGE.end() as u128,
            0..data.len() as u32,
            &mut positions,
        );
        positions
    });
}

#[bench]
fn bench_intfastfield_getrange_u128_single_hit(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let column = get_u128_column_from_data(&data);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(
            *SINGLE_ITEM_RANGE.start() as u128..=*SINGLE_ITEM_RANGE.end() as u128,
            0..data.len() as u32,
            &mut positions,
        );
        positions
    });
}

#[bench]
fn bench_intfastfield_getrange_u128_hit_all(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let column = get_u128_column_from_data(&data);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(0..=u128::MAX, 0..data.len() as u32, &mut positions);
        positions
    });
}
// U128 RANGE END

#[bench]
fn bench_intfastfield_scan_all_fflookup_u128(b: &mut Bencher) {
    let column = get_u128_column_random();

    b.iter(|| {
        let mut a = 0u128;
        for i in 0u64..column.num_vals() as u64 {
            a += column.get_val(i as u32);
        }
        a
    });
}

#[bench]
fn bench_intfastfield_jumpy_stride5_u128(b: &mut Bencher) {
    let column = get_u128_column_random();

    b.iter(|| {
        let n = column.num_vals();
        let mut a = 0u128;
        for i in (0..n / 5).map(|val| val * 5) {
            a += column.get_val(i);
        }
        a
    });
}
