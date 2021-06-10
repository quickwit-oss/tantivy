#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use fastfield_codecs::{
        bitpacked::{BitpackedFastFieldReader, BitpackedFastFieldSerializer},
        linearinterpol::{LinearInterpolFastFieldSerializer, LinearinterpolFastFieldReader},
        multilinearinterpol::{
            MultiLinearInterpolFastFieldSerializer, MultiLinearinterpolFastFieldReader,
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

    use test::Bencher;
    #[bench]
    fn bench_fastfield_bitpack_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        b.iter(|| {
            let mut out = vec![];
            BitpackedFastFieldSerializer::create(
                &mut out,
                &data,
                stats_from_vec(&data),
                data.iter().cloned(),
            )
            .unwrap();
            out
        });
    }
    #[bench]
    fn bench_fastfield_linearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        b.iter(|| {
            let mut out = vec![];
            LinearInterpolFastFieldSerializer::create(
                &mut out,
                &data,
                stats_from_vec(&data),
                data.iter().cloned(),
                data.iter().cloned(),
            )
            .unwrap();
            out
        });
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_create(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        b.iter(|| {
            let mut out = vec![];
            MultiLinearInterpolFastFieldSerializer::create(
                &mut out,
                &data,
                stats_from_vec(&data),
                data.iter().cloned(),
                data.iter().cloned(),
            )
            .unwrap();
            out
        });
    }
    fn value_iter() -> impl Iterator<Item = u64> {
        0..20_000
    }
    #[bench]
    fn bench_fastfield_bitpack_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        let mut bytes = vec![];
        BitpackedFastFieldSerializer::create(
            &mut bytes,
            &data,
            stats_from_vec(&data),
            data.iter().cloned(),
        )
        .unwrap();
        let reader = BitpackedFastFieldReader::open_from_bytes(&bytes).unwrap();
        b.iter(|| {
            for pos in value_iter() {
                reader.get_u64(pos as u64, &bytes);
            }
        });
    }
    #[bench]
    fn bench_fastfield_linearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        let mut bytes = vec![];
        LinearInterpolFastFieldSerializer::create(
            &mut bytes,
            &data,
            stats_from_vec(&data),
            data.iter().cloned(),
            data.iter().cloned(),
        )
        .unwrap();
        let reader = LinearinterpolFastFieldReader::open_from_bytes(&bytes).unwrap();
        b.iter(|| {
            for pos in value_iter() {
                reader.get_u64(pos as u64, &bytes);
            }
        });
    }
    #[bench]
    fn bench_fastfield_multilinearinterpol_get(b: &mut Bencher) {
        let data: Vec<_> = get_data();
        let mut bytes = vec![];
        MultiLinearInterpolFastFieldSerializer::create(
            &mut bytes,
            &data,
            stats_from_vec(&data),
            data.iter().cloned(),
            data.iter().cloned(),
        )
        .unwrap();
        let reader = MultiLinearinterpolFastFieldReader::open_from_bytes(&bytes).unwrap();
        b.iter(|| {
            for pos in value_iter() {
                reader.get_u64(pos as u64, &bytes);
            }
        });
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
