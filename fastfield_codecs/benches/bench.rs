#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use fastfield_codecs::*;
    use rand::prelude::*;

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
                a = column.get_val(a as u64);
            }
            a
        });
    }

    fn get_exp_data() -> Vec<u64> {
        let mut data = vec![];
        for i in 0..100 {
            let num = i * i;
            data.extend(iter::repeat(i as u64).take(num));
        }
        data.shuffle(&mut StdRng::from_seed([1u8; 32]));

        // lengt = 328350
        data
    }

    fn get_data_50percent_item() -> (u128, u128, Vec<u128>) {
        let mut permutation = get_exp_data();
        let major_item = 20;
        let minor_item = 10;
        permutation.extend(iter::repeat(major_item).take(permutation.len()));
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
        (major_item as u128, minor_item as u128, permutation)
    }
    fn get_u128_column_random() -> Arc<dyn ColumnExt<u128>> {
        let permutation = generate_random();
        let permutation = permutation.iter().map(|el| *el as u128).collect::<Vec<_>>();
        get_u128_column_from_data(&permutation)
    }

    fn get_u128_column_from_data(data: &[u128]) -> Arc<dyn ColumnExt<u128>> {
        let mut out = vec![];
        serialize_u128(VecColumn::from(&data), &mut out).unwrap();
        let out = OwnedBytes::new(out);
        open_u128(out).unwrap()
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_50percent_hit(b: &mut Bencher) {
        let (major_item, _minor_item, data) = get_data_50percent_item();
        let column = get_u128_column_from_data(&data);

        b.iter(|| column.get_between_vals(major_item..=major_item));
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_single_hit(b: &mut Bencher) {
        let (_major_item, minor_item, data) = get_data_50percent_item();
        let column = get_u128_column_from_data(&data);

        b.iter(|| column.get_between_vals(minor_item..=minor_item));
    }

    #[bench]
    fn bench_intfastfield_getrange_u128_hit_all(b: &mut Bencher) {
        let (_major_item, _minor_item, data) = get_data_50percent_item();
        let column = get_u128_column_from_data(&data);

        b.iter(|| column.get_between_vals(0..=u128::MAX));
    }

    #[bench]
    fn bench_intfastfield_scan_all_fflookup_u128(b: &mut Bencher) {
        let column = get_u128_column_random();

        b.iter(|| {
            let mut a = 0u128;
            for i in 0u64..column.num_vals() as u64 {
                a += column.get_val(i);
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
                a += column.get_val(i as u64);
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
            let mut a = 0u64;
            for i in (0..n / 7).map(|val| val * 7) {
                a += column.get_val(i as u64);
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
            for i in 0u64..n as u64 {
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
            for i in 0..n as u64 {
                a += column.get_val(i);
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

    fn get_data() -> Vec<u64> {
        let mut rng = StdRng::seed_from_u64(2u64);
        let mut data: Vec<_> = (100..55000_u64)
            .map(|num| num + rng.gen::<u8>() as u64)
            .collect();
        data.push(99_000);
        data.insert(1000, 2000);
        data.insert(2000, 100);
        data.insert(3000, 4100);
        data.insert(4000, 100);
        data.insert(5000, 800);
        data
    }

    #[inline(never)]
    fn value_iter() -> impl Iterator<Item = u64> {
        0..20_000
    }
    fn get_reader_for_bench<Codec: FastFieldCodec>(data: &[u64]) -> Codec::Reader {
        let mut bytes = Vec::new();
        let min_value = *data.iter().min().unwrap();
        let data = data.iter().map(|el| *el - min_value).collect::<Vec<_>>();
        let col = VecColumn::from(&data);
        let normalized_header = fastfield_codecs::NormalizedHeader {
            num_vals: col.num_vals(),
            max_value: col.max_value(),
        };
        Codec::serialize(&VecColumn::from(&data), &mut bytes).unwrap();
        Codec::open_from_bytes(OwnedBytes::new(bytes), normalized_header).unwrap()
    }
    fn bench_get<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let col = get_reader_for_bench::<Codec>(data);
        b.iter(|| {
            let mut sum = 0u64;
            for pos in value_iter() {
                let val = col.get_val(pos as u64);
                sum = sum.wrapping_add(val);
            }
            sum
        });
    }

    #[inline(never)]
    fn bench_get_dynamic_helper(b: &mut Bencher, col: Arc<dyn Column>) {
        b.iter(|| {
            let mut sum = 0u64;
            for pos in value_iter() {
                let val = col.get_val(pos as u64);
                sum = sum.wrapping_add(val);
            }
            sum
        });
    }

    fn bench_get_dynamic<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let col = Arc::new(get_reader_for_bench::<Codec>(data));
        bench_get_dynamic_helper(b, col);
    }
    fn bench_create<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let min_value = *data.iter().min().unwrap();
        let data = data.iter().map(|el| *el - min_value).collect::<Vec<_>>();

        let mut bytes = Vec::new();
        b.iter(|| {
            bytes.clear();
            Codec::serialize(&VecColumn::from(&data), &mut bytes).unwrap();
        });
    }

    use ownedbytes::OwnedBytes;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;
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
}
