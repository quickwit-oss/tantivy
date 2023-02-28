#![feature(test)]
extern crate test;

use std::ops::RangeInclusive;
use std::sync::Arc;

use rand::prelude::*;
use tantivy_columnar::column_values::{serialize_and_load_u64_based_column_values, CodecType};
use tantivy_columnar::*;
use test::Bencher;

// Warning: this generates the same permutation at each call
fn generate_permutation() -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

fn generate_random() -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..100_000u64)
        .map(|el| el + random::<u16>() as u64)
        .collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

// Warning: this generates the same permutation at each call
fn generate_permutation_gcd() -> Vec<u64> {
    let mut permutation: Vec<u64> = (1u64..100_000u64).map(|el| el * 1000).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

pub fn serialize_and_load(column: &[u64], codec_type: CodecType) -> Arc<dyn ColumnValues<u64>> {
    serialize_and_load_u64_based_column_values(&column, &[codec_type])
}

#[bench]
fn bench_intfastfield_jumpy_veclookup(b: &mut Bencher) {
    let permutation = generate_permutation();
    let n = permutation.len();
    b.iter(|| {
        let mut a = 0u64;
        for _ in 0..n {
            a = permutation[a as usize];
        }
        a
    });
}

#[bench]
fn bench_intfastfield_jumpy_fflookup_bitpacked(b: &mut Bencher) {
    let permutation = generate_permutation();
    let n = permutation.len();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&permutation, CodecType::Bitpacked);
    b.iter(|| {
        let mut a = 0u64;
        for _ in 0..n {
            a = column.get_val(a as u32);
        }
        a
    });
}

const FIFTY_PERCENT_RANGE: RangeInclusive<u64> = 1..=50;
const SINGLE_ITEM: u64 = 90;
const SINGLE_ITEM_RANGE: RangeInclusive<u64> = 90..=90;
const ONE_PERCENT_ITEM_RANGE: RangeInclusive<u64> = 49..=49;
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

// U64 RANGE START
#[bench]
fn bench_intfastfield_getrange_u64_50percent_hit(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&data, CodecType::Bitpacked);
    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(
            FIFTY_PERCENT_RANGE,
            0..data.len() as u32,
            &mut positions,
        );
        positions
    });
}

#[bench]
fn bench_intfastfield_getrange_u64_1percent_hit(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&data, CodecType::Bitpacked);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(
            ONE_PERCENT_ITEM_RANGE,
            0..data.len() as u32,
            &mut positions,
        );
        positions
    });
}

#[bench]
fn bench_intfastfield_getrange_u64_single_hit(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&data, CodecType::Bitpacked);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(SINGLE_ITEM_RANGE, 0..data.len() as u32, &mut positions);
        positions
    });
}

#[bench]
fn bench_intfastfield_getrange_u64_hit_all(b: &mut Bencher) {
    let data = get_data_50percent_item();
    let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&data, CodecType::Bitpacked);

    b.iter(|| {
        let mut positions = Vec::new();
        column.get_row_ids_for_value_range(0..=u64::MAX, 0..data.len() as u32, &mut positions);
        positions
    });
}
// U64 RANGE END

#[bench]
fn bench_intfastfield_stride7_vec(b: &mut Bencher) {
    let permutation = generate_permutation();
    let n = permutation.len();
    b.iter(|| {
        let mut a = 0u64;
        for i in (0..n / 7).map(|val| val * 7) {
            a += permutation[i as usize];
        }
        a
    });
}

#[bench]
fn bench_intfastfield_stride7_fflookup(b: &mut Bencher) {
    let permutation = generate_permutation();
    let n = permutation.len();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&permutation, CodecType::Bitpacked);
    b.iter(|| {
        let mut a = 0;
        for i in (0..n / 7).map(|val| val * 7) {
            a += column.get_val(i as u32);
        }
        a
    });
}

#[bench]
fn bench_intfastfield_scan_all_fflookup(b: &mut Bencher) {
    let permutation = generate_permutation();
    let n = permutation.len();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&permutation, CodecType::Bitpacked);
    let column_ref = column.as_ref();
    b.iter(|| {
        let mut a = 0u64;
        for i in 0u32..n as u32 {
            a += column_ref.get_val(i);
        }
        a
    });
}

#[bench]
fn bench_intfastfield_scan_all_fflookup_gcd(b: &mut Bencher) {
    let permutation = generate_permutation_gcd();
    let n = permutation.len();
    let column: Arc<dyn ColumnValues<u64>> = serialize_and_load(&permutation, CodecType::Bitpacked);
    b.iter(|| {
        let mut a = 0u64;
        for i in 0..n {
            a += column.get_val(i as u32);
        }
        a
    });
}

#[bench]
fn bench_intfastfield_scan_all_vec(b: &mut Bencher) {
    let permutation = generate_permutation();
    b.iter(|| {
        let mut a = 0u64;
        for i in 0..permutation.len() {
            a += permutation[i as usize] as u64;
        }
        a
    });
}
