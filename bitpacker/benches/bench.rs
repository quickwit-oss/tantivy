#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use tantivy_bitpacker::BlockedBitpacker;
    use test::Bencher;
    #[bench]
    fn bench_blockedbitp_read(b: &mut Bencher) {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..=21500 {
            blocked_bitpacker.add(val);
        }
        b.iter(|| {
            for val in 0..=21500 {
                blocked_bitpacker.get(val);
            }
        });
    }
    #[bench]
    fn bench_blockbitp_create(b: &mut Bencher) {
        b.iter(|| {
            let mut blocked_bitpacker = BlockedBitpacker::new();
            for val in 0..=21500 {
                blocked_bitpacker.add(val);
            }
        });
    }
}
