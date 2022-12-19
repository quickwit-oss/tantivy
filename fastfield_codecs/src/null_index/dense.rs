use std::convert::TryInto;
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
#[derive(Clone)]
pub struct DenseCodec {
    // data consists of blocks of 64 bits.
    //
    // The format is &[(u64, u32)]
    // u64 is the bitvec
    // u32 is the offset of the block, the number of set bits so far.
    //
    // At the end one block is appended, to store the number of values in the index in offset.
    data: OwnedBytes,
}
const ELEMENTS_PER_BLOCK: u32 = 64;
const BLOCK_BITVEC_SIZE: usize = 8;
const BLOCK_OFFSET_SIZE: usize = 4;
const SERIALIZED_BLOCK_SIZE: usize = BLOCK_BITVEC_SIZE + BLOCK_OFFSET_SIZE;

#[inline]
fn count_ones(bitvec: u64, pos_in_bitvec: u32) -> u32 {
    if pos_in_bitvec == 63 {
        bitvec.count_ones()
    } else {
        let mask = (1u64 << (pos_in_bitvec + 1)) - 1;
        let masked_bitvec = bitvec & mask;
        masked_bitvec.count_ones()
    }
}

#[derive(Clone, Copy)]
struct DenseIndexBlock {
    bitvec: u64,
    offset: u32,
}

impl From<[u8; SERIALIZED_BLOCK_SIZE]> for DenseIndexBlock {
    fn from(data: [u8; SERIALIZED_BLOCK_SIZE]) -> Self {
        let bitvec = u64::from_le_bytes(data[..BLOCK_BITVEC_SIZE].try_into().unwrap());
        let offset = u32::from_le_bytes(data[BLOCK_BITVEC_SIZE..].try_into().unwrap());
        Self { bitvec, offset }
    }
}

impl DenseCodec {
    /// Open the DenseCodec from OwnedBytes
    pub fn open(data: OwnedBytes) -> Self {
        Self { data }
    }
    #[inline]
    /// Check if value at position is not null.
    pub fn exists(&self, idx: u32) -> bool {
        let block_pos = idx / ELEMENTS_PER_BLOCK;
        let bitvec = self.dense_index_block(block_pos).bitvec;

        let pos_in_bitvec = idx % ELEMENTS_PER_BLOCK;

        get_bit_at(bitvec, pos_in_bitvec)
    }
    #[inline]
    fn dense_index_block(&self, block_pos: u32) -> DenseIndexBlock {
        dense_index_block(&self.data, block_pos)
    }

    /// Return the number of non-null values in an index
    pub fn num_non_nulls(&self) -> u32 {
        let last_block = (self.data.len() / SERIALIZED_BLOCK_SIZE) - 1;
        self.dense_index_block(last_block as u32).offset
    }

    #[inline]
    /// Translate from the original index to the codec index.
    pub fn translate_to_codec_idx(&self, idx: u32) -> Option<u32> {
        let block_pos = idx / ELEMENTS_PER_BLOCK;
        let index_block = self.dense_index_block(block_pos);
        let pos_in_block_bit_vec = idx % ELEMENTS_PER_BLOCK;
        let ones_in_block = count_ones(index_block.bitvec, pos_in_block_bit_vec);
        if get_bit_at(index_block.bitvec, pos_in_block_bit_vec) {
            // -1 is ok, since idx does exist, so there's at least one
            Some(index_block.offset + ones_in_block - 1)
        } else {
            None
        }
    }

    /// Translate positions from the codec index to the original index.
    ///
    /// # Panics
    ///
    /// May panic if any `idx` is greater than the max codec index.
    pub fn translate_codec_idx_to_original_idx<'a>(
        &'a self,
        iter: impl Iterator<Item = u32> + 'a,
    ) -> impl Iterator<Item = u32> + 'a {
        let mut block_pos = 0u32;
        iter.map(move |dense_idx| {
            // update block_pos to limit search scope
            block_pos = find_block(dense_idx, block_pos, &self.data);
            let index_block = self.dense_index_block(block_pos);

            // The next offset is higher than dense_idx and therefore:
            // dense_idx <= offset + num_set_bits in block
            let mut num_set_bits = 0;
            for idx_in_bitvec in 0..ELEMENTS_PER_BLOCK {
                if get_bit_at(index_block.bitvec, idx_in_bitvec) {
                    num_set_bits += 1;
                }
                if num_set_bits == (dense_idx - index_block.offset + 1) {
                    let orig_idx = block_pos * ELEMENTS_PER_BLOCK + idx_in_bitvec;
                    return orig_idx;
                }
            }
            panic!("Internal Error: Offset calculation in dense idx seems to be wrong.");
        })
    }
}

#[inline]
fn dense_index_block(data: &[u8], block_pos: u32) -> DenseIndexBlock {
    let data_start_pos = block_pos as usize * SERIALIZED_BLOCK_SIZE;
    let block_data: [u8; SERIALIZED_BLOCK_SIZE] = data[data_start_pos..][..SERIALIZED_BLOCK_SIZE]
        .try_into()
        .unwrap();
    block_data.into()
}

#[inline]
/// Finds the block position containing the dense_idx.
///
/// # Correctness
/// dense_idx needs to be smaller than the number of values in the index
///
/// The last offset number is equal to the number of values in the index.
fn find_block(dense_idx: u32, mut block_pos: u32, data: &[u8]) -> u32 {
    loop {
        let offset = dense_index_block(data, block_pos).offset;
        if offset > dense_idx {
            return block_pos - 1;
        }
        block_pos += 1;
    }
}

/// Iterator over all values, true if set, otherwise false
pub fn serialize_dense_codec(
    iter: impl Iterator<Item = bool>,
    mut out: impl Write,
) -> io::Result<()> {
    let mut offset: u32 = 0;

    for chunk in &iter.chunks(ELEMENTS_PER_BLOCK as usize) {
        let mut block: u64 = 0;
        for (pos, is_bit_set) in chunk.enumerate() {
            if is_bit_set {
                set_bit_at(&mut block, pos as u64);
            }
        }

        block.serialize(&mut out)?;
        offset.serialize(&mut out)?;

        offset += block.count_ones();
    }
    // Add sentinal block for the offset
    let block: u64 = 0;
    block.serialize(&mut out)?;
    offset.serialize(&mut out)?;

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
        let null_index = DenseCodec::open(OwnedBytes::new(out));

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
        let null_index = DenseCodec::open(OwnedBytes::new(out));

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
        let null_index = DenseCodec::open(OwnedBytes::new(out));
        assert_eq!(null_index.translate_to_codec_idx(0), Some(0));
        assert_eq!(null_index.translate_to_codec_idx(2), Some(1));
    }

    #[test]
    fn dense_codec_test_small() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_dense_codec(iter, &mut out).unwrap();
        let null_index = DenseCodec::open(OwnedBytes::new(out));
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
        let null_index = DenseCodec::open(OwnedBytes::new(out));
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

    const TOTAL_NUM_VALUES: u32 = 1_000_000;
    fn gen_bools(fill_ratio: f64) -> DenseCodec {
        let mut out = Vec::new();
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        let bools: Vec<_> = (0..TOTAL_NUM_VALUES)
            .map(|_| rng.gen_bool(fill_ratio))
            .collect();
        serialize_dense_codec(bools.into_iter(), &mut out).unwrap();

        let codec = DenseCodec::open(OwnedBytes::new(out));
        codec
    }

    fn random_range_iterator(start: u32, end: u32, step_size: u32) -> impl Iterator<Item = u32> {
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        let mut current = start;
        std::iter::from_fn(move || {
            current += rng.gen_range(1..step_size + 1);
            if current >= end {
                None
            } else {
                Some(current)
            }
        })
    }

    fn walk_over_data(codec: &DenseCodec, max_step_size: u32) -> Option<u32> {
        walk_over_data_from_positions(
            codec,
            random_range_iterator(0, TOTAL_NUM_VALUES, max_step_size),
        )
    }

    fn walk_over_data_from_positions(
        codec: &DenseCodec,
        positions: impl Iterator<Item = u32>,
    ) -> Option<u32> {
        let mut dense_idx: Option<u32> = None;
        for idx in positions {
            dense_idx = dense_idx.or(codec.translate_to_codec_idx(idx));
        }
        dense_idx
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_codec_90percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_codec_50percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.5f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_codec_full_scan_10percent(bench: &mut Bencher) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_codec_full_scan_90percent(bench: &mut Bencher) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_dense_codec_translate_orig_to_codec_10percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_dense_codec_translate_codec_to_orig_90percent_filled_random_stride_big_step(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.9f64);
        let num_vals = codec.num_non_nulls();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(random_range_iterator(0, num_vals, 50_000))
                .last()
        });
    }

    #[bench]
    fn bench_dense_codec_translate_codec_to_orig_90percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.9f64);
        let num_vals = codec.num_non_nulls();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(random_range_iterator(0, num_vals, 100))
                .last()
        });
    }

    #[bench]
    fn bench_dense_codec_translate_codec_to_orig_90percent_filled_full_scan(bench: &mut Bencher) {
        let codec = gen_bools(0.9f64);
        let num_vals = codec.num_non_nulls();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(0..num_vals)
                .last()
        });
    }
}
