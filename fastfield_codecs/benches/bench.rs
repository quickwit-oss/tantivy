#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use common::OwnedBytes;
    use fastfield_codecs::*;
    use rand::prelude::*;
    use test::Bencher;

    use super::*;

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

    pub fn serialize_and_load<T: MonotonicallyMappableToU64 + Ord + Default>(
        column: &[T],
    ) -> Arc<dyn Column<T>> {
        let mut buffer = Vec::new();
        serialize(VecColumn::from(&column), &mut buffer, &ALL_CODEC_TYPES).unwrap();
        open(OwnedBytes::new(buffer)).unwrap()
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
    fn bench_intfastfield_jumpy_fflookup(b: &mut Bencher) {
        let permutation = generate_permutation();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = serialize_and_load(&permutation);
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
    fn get_u128_column_random() -> Arc<dyn Column<u128>> {
        let permutation = generate_random();
        let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
        get_u128_column_from_data(&permutation)
    }

    fn get_u128_column_from_data(data: &[u128]) -> Arc<dyn Column<u128>> {
        let mut out = vec![];
        let iter_gen = || data.iter().cloned();
        serialize_u128(iter_gen, data.len() as u32, &mut out).unwrap();
        let out = OwnedBytes::new(out);
        open_u128::<u128>(out).unwrap()
    }

    // U64 RANGE START
    #[bench]
    fn bench_intfastfield_getrange_u64_50percent_hit(b: &mut Bencher) {
        let data = get_data_50percent_item();
        let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
        let column: Arc<dyn Column<u64>> = serialize_and_load(&data);

        b.iter(|| {
            let mut positions = Vec::new();
            column.get_docids_for_value_range(
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
        let column: Arc<dyn Column<u64>> = serialize_and_load(&data);

        b.iter(|| {
            let mut positions = Vec::new();
            column.get_docids_for_value_range(
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
        let column: Arc<dyn Column<u64>> = serialize_and_load(&data);

        b.iter(|| {
            let mut positions = Vec::new();
            column.get_docids_for_value_range(
                SINGLE_ITEM_RANGE,
                0..data.len() as u32,
                &mut positions,
            );
            positions
        });
    }

    #[bench]
    fn bench_intfastfield_getrange_u64_hit_all(b: &mut Bencher) {
        let data = get_data_50percent_item();
        let data = data.iter().map(|el| *el as u64).collect::<Vec<_>>();
        let column: Arc<dyn Column<u64>> = serialize_and_load(&data);

        b.iter(|| {
            let mut positions = Vec::new();
            column.get_docids_for_value_range(0..=u64::MAX, 0..data.len() as u32, &mut positions);
            positions
        });
    }
    // U64 RANGE END

    // U128 RANGE START
    #[bench]
    fn bench_intfastfield_getrange_u128_50percent_hit(b: &mut Bencher) {
        let data = get_data_50percent_item();
        let column = get_u128_column_from_data(&data);

        b.iter(|| {
            let mut positions = Vec::new();
            column.get_docids_for_value_range(
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
            column.get_docids_for_value_range(
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
            column.get_docids_for_value_range(0..=u128::MAX, 0..data.len() as u32, &mut positions);
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
        let column: Arc<dyn Column<u64>> = serialize_and_load(&permutation);
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
        let column: Arc<dyn Column<u64>> = serialize_and_load(&permutation);
        b.iter(|| {
            let mut a = 0u64;
            for i in 0u32..n as u32 {
                a += column.get_val(i);
            }
            a
        });
    }

    #[bench]
    fn bench_intfastfield_scan_all_fflookup_gcd(b: &mut Bencher) {
        let permutation = generate_permutation_gcd();
        let n = permutation.len();
        let column: Arc<dyn Column<u64>> = serialize_and_load(&permutation);
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
}
