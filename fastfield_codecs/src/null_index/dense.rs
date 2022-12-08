use std::io::{self, Write};

use common::BinarySerializable;
use itertools::Itertools;
use ownedbytes::OwnedBytes;

use super::{get_bit_at, set_bit_at};

/// For the `DenseCodec`, `data` which contains the encoded blocks.
/// Each block consists of [u8; 12]. The first 8 bytes is a bitvec for 64 elements.
/// The last 4 bytes are the offset, the number of set bits so far.
///
/// When translating the original index to a dense index, the correct block can be computed
/// directly `orig_idx/64`. Inside the block the position is `orig_idx%64`.
///
/// When translating a dense index to the original index, we can use the offset to find the correct
/// block. Direct computation is not possible, but we can employ a linear or binary search.
pub struct DenseCodec {
    // data consists of blocks of 64 bits.
    //
    // The format is &[(u64, u32)]
    // u64 is the bitvec
    // u32 is the offset of the block, the number of set bits so far.
    //
    // At the end one block is appended, to store the number of values in the index in offset.
    data: Vec<IndexBlock>,
}
const ELEMENTS_PER_BLOCK: u32 = 32;

#[inline]
fn count_ones(block: u32, pos_in_block: u32) -> u32 {
    unsafe { core::arch::x86_64::_bzhi_u32(block, pos_in_block + 1) }.count_ones()
    // if pos_in_block == 31 {
    //     block.count_ones()
    // } else {
    //     let mask = (1u32 << (pos_in_block + 1)) - 1;
    //     let masked_block = block & mask;
    //     masked_block.count_ones()
    // }
}

#[derive(Copy, Clone, Debug)]
pub struct IndexBlock {
    bitvec: u32,
    offset: u32,
}

impl DenseCodec {
    /// Open the DenseCodec from OwnedBytes
    pub fn open(data: Vec<IndexBlock>) -> Self {
        Self { data }
    }
    #[inline]
    /// Check if value at position is not null.
    pub fn exists(&self, idx: u32) -> bool {
        let block_pos = idx / ELEMENTS_PER_BLOCK;
        let bitvec: u32 = self.block(block_pos);
        let pos_in_block = idx % ELEMENTS_PER_BLOCK;
        get_bit_at(bitvec, pos_in_block)
    }

    #[inline]
    pub(crate) fn block(&self, block_pos: u32) -> u32 {
        self.block_and_offset(block_pos).bitvec
    }

    #[inline]
    /// Returns (bitvec, offset)
    ///
    /// offset is the start offset of actual docids in the block.
    pub(crate) fn block_and_offset(&self, block_pos: u32) -> IndexBlock {
        self.data[block_pos as usize]
    }

    /// Return the number of non-null values in an index
    pub fn num_non_null_vals(&self) -> u32 {
        let last_block = self.data.len() - 1;
        self.block_and_offset(last_block as u32).offset
    }

    #[inline]
    /// Translate from the original index to the codec index.
    pub fn translate_to_codec_idx(&self, idx: u32) -> Option<u32> {
        let block_pos = idx / ELEMENTS_PER_BLOCK;
        let IndexBlock { bitvec: block, offset } = self.block_and_offset(block_pos);
        let pos_in_block = idx % ELEMENTS_PER_BLOCK;
        let ones_in_block = count_ones(block, pos_in_block);
        if get_bit_at(block, pos_in_block) {
            Some(offset + ones_in_block - 1) // -1 is ok, since idx does exist, so there's at least
                                             // one
        } else {
            None
        }
    }

    /// Translate positions from the codec index to the original index.
    ///
    /// # Panics
    ///
    /// May panic if any `idx` is greater than the column length.
    pub fn translate_codec_idx_to_original_idx<'a>(
        &'a self,
        iter: impl Iterator<Item = u32> + 'a,
    ) -> impl Iterator<Item = u32> + 'a {
        let mut block_pos = 0u32;
        iter.map(move |dense_idx| {
            // update block_pos to limit search scope
            block_pos = find_block(dense_idx, block_pos, &self.data);
            let IndexBlock { bitvec, offset} = self.block_and_offset(block_pos);
            // The next offset is higher than dense_idx and therefore:
            // dense_idx <= offset + num_set_bits in block
            let mut num_set_bits = 0;
            for idx_in_block in 0..ELEMENTS_PER_BLOCK {
                if get_bit_at(bitvec, idx_in_block) {
                    num_set_bits += 1;
                }
                if num_set_bits == (dense_idx - offset + 1) {
                    let orig_idx = block_pos * ELEMENTS_PER_BLOCK + idx_in_block as u32;
                    return orig_idx;
                }
            }
            panic!("Internal Error: Offset calculation in dense idx seems to be wrong.");
        })
    }
}

#[inline]
/// Finds the block position containing the dense_idx.
///
/// # Correctness
/// dense_idx needs to be smaller than the number of values in the index
///
/// The last offset number is equal to the number of values in the index.
fn find_block(dense_idx: u32, mut block_pos: u32, data: &[IndexBlock]) -> u32 {
    for i in block_pos.. {
        let index_block = &data[i as usize];
        if index_block.offset > dense_idx {
            // offset
            return i - 1;
        }
    }
    unreachable!()
}

/// Iterator over all values, true if set, otherwise false
pub fn serialize_dense_codec(
    iter: impl Iterator<Item = bool>,
    out: &mut Vec<IndexBlock>,
) -> io::Result<()> {
    let mut offset: u32 = 0;

    for chunk in &iter.chunks(ELEMENTS_PER_BLOCK as usize) {
        let mut bitvec: u32 = 0;
        for (pos, is_bit_set) in chunk.enumerate() {
            if is_bit_set {
                set_bit_at(&mut bitvec, pos as u32);
            }
        }
        out.push(IndexBlock { bitvec, offset});
        offset += bitvec.count_ones() as u32;
    }
    // Add sentinal block for the offset
    out.push(IndexBlock { bitvec: 0, offset });

    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::{any, prop, *};
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use super::*;

    fn random_bitvec() -> BoxedStrategy<Vec<bool>> {
        prop_oneof![
            1 => prop::collection::vec(proptest::bool::weighted(1.0), 0..100),
            1 => prop::collection::vec(proptest::bool::weighted(1.0), 0..64),
            1 => prop::collection::vec(proptest::bool::weighted(0.0), 0..100),
            1 => prop::collection::vec(proptest::bool::weighted(0.0), 0..64),
            8 => vec![any::<bool>()],
            2 => prop::collection::vec(any::<bool>(), 0..50),
        ]
        .boxed()
    }

        #[test]
        fn test_with_random_bitvecs_simple() {
            let mut bitvec = Vec::new();
            bitvec.extend_from_slice(&[]);
            bitvec.extend_from_slice(&[]);
            bitvec.extend_from_slice(&[true]);
            test_null_index(bitvec);
        }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn test_with_random_bitvecs(bitvec1 in random_bitvec(), bitvec2 in random_bitvec(), bitvec3 in random_bitvec()) {
            let mut bitvec = Vec::new();
            bitvec.extend_from_slice(&bitvec1);
            bitvec.extend_from_slice(&bitvec2);
            bitvec.extend_from_slice(&bitvec3);
            test_null_index(bitvec);
        }
    }

    #[test]
    fn dense_codec_test_one_block_false() {
        let mut iter = vec![false; 64];
        iter.push(true);
        test_null_index(iter);
    }

    fn test_null_index(data: Vec<bool>) {
        let mut out = vec![];

        serialize_dense_codec(data.iter().cloned(), &mut out).unwrap();
        dbg!(&out);
        let null_index = DenseCodec::open(out);

        let orig_idx_with_value: Vec<u32> = data
            .iter()
            .enumerate()
            .filter(|(_pos, val)| **val)
            .map(|(pos, _val)| pos as u32)
            .collect();

        assert_eq!(
            null_index
                .translate_codec_idx_to_original_idx(0..orig_idx_with_value.len() as u32)
                .collect_vec(),
            orig_idx_with_value
        );

        for (dense_idx, orig_idx) in orig_idx_with_value.iter().enumerate() {
            assert_eq!(
                null_index.translate_to_codec_idx(*orig_idx),
                Some(dense_idx as u32)
            );
        }

        for (pos, value) in data.iter().enumerate() {
            assert_eq!(null_index.exists(pos as u32), *value);
        }
    }

    #[test]
    fn dense_codec_test_translation() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_dense_codec(iter, &mut out).unwrap();
        let null_index = DenseCodec::open(out);

        assert_eq!(
            null_index
                .translate_codec_idx_to_original_idx(0..2)
                .collect_vec(),
            vec![0, 2]
        );
    }

    #[test]
    fn dense_codec_translate() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_dense_codec(iter, &mut out).unwrap();
        let null_index = DenseCodec::open(out);
        assert_eq!(null_index.translate_to_codec_idx(0), Some(0));
        assert_eq!(null_index.translate_to_codec_idx(2), Some(1));
    }

    #[test]
    fn dense_codec_test_small() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_dense_codec(iter, &mut out).unwrap();
        let null_index = DenseCodec::open(out);
        assert!(null_index.exists(0));
        assert!(!null_index.exists(1));
        assert!(null_index.exists(2));
        assert!(!null_index.exists(3));
    }

    #[test]
    fn dense_codec_test_large() {
        let mut docs = vec![];
        docs.extend((0..1000).map(|_idx| false));
        docs.extend((0..=1000).map(|_idx| true));

        let iter = docs.iter().cloned();
        let mut out = vec![];
        serialize_dense_codec(iter, &mut out).unwrap();
        let null_index = DenseCodec::open(out);
        assert!(!null_index.exists(0));
        assert!(!null_index.exists(100));
        assert!(!null_index.exists(999));
        assert!(null_index.exists(1000));
        assert!(null_index.exists(1999));
        assert!(null_index.exists(2000));
        assert!(!null_index.exists(2001));
    }

    #[test]
    fn test_count_ones() {
        let mut block = 0;
        set_bit_at(&mut block, 0);
        set_bit_at(&mut block, 2);

        assert_eq!(count_ones(block, 0), 1);
        assert_eq!(count_ones(block, 1), 1);
        assert_eq!(count_ones(block, 2), 2);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {


    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;

    use super::*;

    fn gen_bools() -> DenseCodec {
        let mut out = Vec::new();
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        // 80% of values are set
        let bools: Vec<_> = (0..100_000).map(|_| rng.gen_bool(8f64 / 10f64)).collect();
        serialize_dense_codec(bools.into_iter(), &mut out).unwrap();

        let codec = DenseCodec::open(out);
        codec
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_dense(bench: &mut Bencher) {
        let codec = gen_bools();
        bench.iter(|| {
            let mut dense_idx: Option<u32> = None;
            for idx in 0..100_000 {
                dense_idx = dense_idx.or(codec.translate_to_codec_idx(idx));
            }
            dense_idx
        });
    }

    #[bench]
    fn bench_dense_codec_translate_dense_to_orig(bench: &mut Bencher) {
        let codec = gen_bools();
        let num_vals = codec.num_non_null_vals();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(0..num_vals)
                .last()
        });
    }
}
