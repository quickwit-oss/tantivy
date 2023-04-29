#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use rand::seq::IteratorRandom;
    use rand::thread_rng;
    use tantivy_common::serialize_vint_u32;
    use test::Bencher;

    #[bench]
    fn bench_vint(b: &mut Bencher) {
        let vals: Vec<u32> = (0..20_000).collect();
        b.iter(|| {
            let mut out = 0u64;
            for val in vals.iter().cloned() {
                let mut buf = [0u8; 8];
                serialize_vint_u32(val, &mut buf);
                out += u64::from(buf[0]);
            }
            out
        });
    }

    #[bench]
    fn bench_vint_rand(b: &mut Bencher) {
        let vals: Vec<u32> = (0..20_000).choose_multiple(&mut thread_rng(), 100_000);
        b.iter(|| {
            let mut out = 0u64;
            for val in vals.iter().cloned() {
                let mut buf = [0u8; 8];
                serialize_vint_u32(val, &mut buf);
                out += u64::from(buf[0]);
            }
            out
        });
    }
}
