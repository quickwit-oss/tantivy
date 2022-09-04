#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use fastfield_codecs::bitpacked::BitpackedCodec;
    use fastfield_codecs::blockwise_linear::BlockwiseLinearCodec;
    use fastfield_codecs::linear::LinearCodec;
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
    fn bench_get<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let mut bytes = vec![];
        Codec::serialize(&mut bytes, &VecColumn::from(data)).unwrap();
        let reader = Codec::open_from_bytes(OwnedBytes::new(bytes)).unwrap();
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
    fn bench_create<Codec: FastFieldCodec>(b: &mut Bencher, data: &[u64]) {
        let mut bytes = Vec::new();
        b.iter(|| {
            bytes.clear();
            Codec::serialize(&mut bytes, &VecColumn::from(data)).unwrap();
        });
    }

    use ownedbytes::OwnedBytes;
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
    fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<LinearCodec>(b, &data);
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        bench_get::<BlockwiseLinearCodec>(b, &data);
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
