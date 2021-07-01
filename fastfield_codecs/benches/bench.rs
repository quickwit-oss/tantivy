#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use fastfield_codecs::{
        bitpacked::{BitpackedFastFieldReader, BitpackedFastFieldSerializer},
        linearinterpol::{LinearInterpolFastFieldReader, LinearInterpolFastFieldSerializer},
        multilinearinterpol::{
            MultiLinearInterpolFastFieldReader, MultiLinearInterpolFastFieldSerializer,
        },
        *,
    };

    fn get_data() -> Vec<u64> {
        let mut data: Vec<_> = (100..55000_u64)
            .map(|num| num + rand::random::<u8>() as u64)
            .collect();
        data.push(99_000);
        data.insert(1000, 2000);
        data.insert(2000, 100);
        data.insert(3000, 4100);
        data.insert(4000, 100);
        data.insert(5000, 800);
        data
    }

    fn value_iter() -> impl Iterator<Item = u64> {
        0..20_000
    }
    fn bench_get<S: FastFieldCodecSerializer, R: FastFieldCodecReader>(
        b: &mut Bencher,
        data: &[u64],
    ) {
        let mut bytes = vec![];
        S::serialize(
            &mut bytes,
            &data,
            stats_from_vec(data),
            data.iter().cloned(),
            data.iter().cloned(),
        )
        .unwrap();
        let reader = R::open_from_bytes(&bytes).unwrap();
        b.iter(|| {
            for pos in value_iter() {
                reader.get_u64(pos as u64, &bytes);
            }
        });
    }
    fn bench_create<S: FastFieldCodecSerializer>(b: &mut Bencher, data: &[u64]) {
        let mut bytes = vec![];
        b.iter(|| {
            S::serialize(
                &mut bytes,
                &data,
                stats_from_vec(data),
                data.iter().cloned(),
                data.iter().cloned(),
            )
            .unwrap();
        });
    }

    use test::Bencher;
    #[bench]
    fn bench_fastfield_bitpack_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<BitpackedFastFieldSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<LinearInterpolFastFieldSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<MultiLinearInterpolFastFieldSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_bitpack_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BitpackedFastFieldSerializer, BitpackedFastFieldReader>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<LinearInterpolFastFieldSerializer, LinearInterpolFastFieldReader>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<MultiLinearInterpolFastFieldSerializer, MultiLinearInterpolFastFieldReader>(
            b, &data,
        );
    }
    pub fn stats_from_vec(data: &[u64]) -> FastFieldStats {
        let min_value = data.iter().cloned().min().unwrap_or(0);
        let max_value = data.iter().cloned().max().unwrap_or(0);
        FastFieldStats {
            min_value,
            max_value,
            num_vals: data.len() as u64,
        }
    }
}
