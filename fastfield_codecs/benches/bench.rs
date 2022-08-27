#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use fastfield_codecs::bitpacked::{BitpackedReader, BitpackedSerializer};
    use fastfield_codecs::blockwise_linear::{BlockwiseLinearReader, BlockwiseLinearSerializer};
    use fastfield_codecs::linear::{LinearReader, LinearSerializer};
    use fastfield_codecs::*;

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
    fn bench_get<
        S: FastFieldCodecSerializer,
        R: FastFieldCodecDeserializer + FastFieldDataAccess,
    >(
        b: &mut Bencher,
        data: &[u64],
    ) {
        let mut bytes = vec![];
        S::serialize(&mut bytes, &data).unwrap();
        let reader = R::open_from_bytes(OwnedBytes::new(bytes)).unwrap();
        b.iter(|| {
            let mut sum = 0u64;
            for pos in value_iter() {
                let val = reader.get_val(pos as u64);
                debug_assert_eq!(data[pos as usize], val);
                sum = sum.wrapping_add(val);
            }
            sum
        });
    }
    fn bench_create<S: FastFieldCodecSerializer>(b: &mut Bencher, data: &[u64]) {
        let mut bytes = vec![];
        b.iter(|| {
            S::serialize(&mut bytes, &data).unwrap();
        });
    }

    use ownedbytes::OwnedBytes;
    use test::Bencher;
    #[bench]
    fn bench_fastfield_bitpack_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<BitpackedSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<LinearSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_create::<BlockwiseLinearSerializer>(b, &data);
    }
    #[bench]
    fn bench_fastfield_bitpack_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BitpackedSerializer, BitpackedReader>(b, &data);
    }
    #[bench]
    fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<LinearSerializer, LinearReader>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BlockwiseLinearSerializer, BlockwiseLinearReader>(b, &data);
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
