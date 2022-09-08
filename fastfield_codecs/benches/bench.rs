#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fastfield_codecs::*;

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
        let col = VecColumn::from(&data);
        let normalized_header = fastfield_codecs::NormalizedHeader {
            num_vals: col.num_vals(),
            max_value: col.max_value(),
        };
        Codec::serialize(&VecColumn::from(data), &mut bytes).unwrap();
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
        let mut bytes = Vec::new();
        b.iter(|| {
            bytes.clear();
            Codec::serialize(&VecColumn::from(data), &mut bytes).unwrap();
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
