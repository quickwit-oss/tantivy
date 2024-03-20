#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use rand::seq::IteratorRandom;
    use rand::thread_rng;
    use tantivy_bitpacker::{BitPacker, BitUnpacker, BlockedBitpacker};
    use test::Bencher;
    use tantivy_bitpacker::filter_vec;

    #[inline(never)]
    fn create_bitpacked_data(bit_width: u8, num_els: u32) -> Vec<u8> {
        let mut bitpacker = BitPacker::new();
        let mut buffer = Vec::new();
        for _ in 0..num_els {
            // the values do not matter.
            bitpacker.write(0u64, bit_width, &mut buffer).unwrap();
            bitpacker.flush(&mut buffer).unwrap();
        }
        buffer
    }

    #[bench]
    fn bench_bitpacking_read(b: &mut Bencher) {
        let bit_width = 3;
        let num_els = 1_000_000u32;
        let bit_unpacker = BitUnpacker::new(bit_width);
        let data = create_bitpacked_data(bit_width, num_els);
        let idxs: Vec<u32> = (0..num_els).choose_multiple(&mut thread_rng(), 100_000);
        b.iter(|| {
            let mut out = 0u64;
            for &idx in &idxs {
                out = out.wrapping_add(bit_unpacker.get(idx, &data[..]));
            }
            out
        });
    }

    #[bench]
    fn bench_blockedbitp_read(b: &mut Bencher) {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..=21500 {
            blocked_bitpacker.add(val * val);
        }
        b.iter(|| {
            let mut out = 0u64;
            for val in 0..=21500 {
                out = out.wrapping_add(blocked_bitpacker.get(val));
            }
            out
        });
    }

    #[bench]
    fn bench_blockedbitp_create(b: &mut Bencher) {
        b.iter(|| {
            let mut blocked_bitpacker = BlockedBitpacker::new();
            for val in 0..=21500 {
                blocked_bitpacker.add(val * val);
            }
            blocked_bitpacker
        });
    }

    fn bench_filter_vec(//values: Vec<u32>,
                        filter_impl: filter_vec::FilterImplPerInstructionSet) -> u32{
        let mut values = vec![0u32; 1_000_000];
        //let mut values = values;
        filter_impl.filter_vec_in_place(0..=10, 0, &mut values);
        values[0]
    }
    #[bench]
    fn bench_filter_vec_avx512(b: &mut Bencher) {
        //let values = vec![0u32; 1_000_000];
        if filter_vec::FilterImplPerInstructionSet::AVX512.is_available() {
            b.iter(|| {
                bench_filter_vec(filter_vec::FilterImplPerInstructionSet::AVX512)
            });
        }
    }
    #[bench]
    fn bench_filter_vec_avx2(b: &mut Bencher) {
        //let values = vec![0u32; 1_000_000];
        if filter_vec::FilterImplPerInstructionSet::AVX2.is_available() {
            b.iter(|| {
                bench_filter_vec(filter_vec::FilterImplPerInstructionSet::AVX2)
            });
        }
    }
    #[bench]
    fn bench_filter_vec_scalar(b: &mut Bencher) {
        //let values = vec![0u32; 1_000_000];
        if filter_vec::FilterImplPerInstructionSet::Scalar.is_available() {
            b.iter(|| {
                bench_filter_vec(filter_vec::FilterImplPerInstructionSet::Scalar)
            });
        }
    }

}
