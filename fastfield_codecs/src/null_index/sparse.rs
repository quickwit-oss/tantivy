use std::io::{self, Write};

use ownedbytes::OwnedBytes;

/// `SparseCodec` is the codec for data, when only few documents have values.
/// In contrast to `DenseCodec` opening a `SparseCodec` causes runtime data to be produced, for
/// faster access.
///
/// The lower 16 bits of doc ids are stored as u16 while the upper 16 bits are given by the block
/// id.
///
/// # Serialized Data Layout
/// The data starts with the raw data of u16 encoded positions of the blocks. [u16 LE, .. #repeat*;
/// Desc: Positions with values in a block]
///
/// The data is followed by block metadata, to know which area of the raw data belongs to which
/// block. Only metadata for blocks with elements is recorded to
/// keep the overhead low for scenarios with many very sparse columns. The block metadata consists
/// of the block index and the number of values in the block.
/// The last u16 is storing the number of metadata blocks.
/// [u16 LE, .. #repeat*; Desc: Positions with values in a block][(u16 LE, u16 LE), .. #repeat*;
/// Desc: (Block Id u16, Num Elements u16)][u16 LE; Desc: num blocks with values u16]
///
/// # Opening
/// When opening the data layout, the data is expanded to `Vec<SparseCodecBlock>`, where the
/// index is the block index.
pub struct SparseCodec {
    data: OwnedBytes,
    blocks: Vec<SparseCodecBlock>,
}

/// A block consists of max u16 values
#[derive(Debug)]
struct SparseCodecBlock {
    /// The number of values in the block
    num_vals: u16,
    /// The number of values set before the block
    offset: u32,
}

impl SparseCodecBlock {
    fn empty_block(offset: u32) -> Self {
        Self {
            num_vals: 0,
            offset,
        }
    }

    fn new(num_vals: u16, offset: u32) -> Self {
        Self { num_vals, offset }
    }
    #[inline]
    fn value_at_idx(&self, data: &[u8], idx: u16) -> u16 {
        let data_start_index = (self.offset + idx as u32) * 2;
        u16::from_le_bytes([
            data[data_start_index as usize],
            data[data_start_index as usize + 1],
        ])
    }

    #[inline]
    #[allow(clippy::comparison_chain)]
    // Looks for the element in the block. Returns the positions if found.
    fn binary_search(&self, data: &[u8], target: u16) -> Option<u16> {
        let mut size = self.num_vals;
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            let mid_val = self.value_at_idx(data, mid);

            if target > mid_val {
                left = mid + 1;
            } else if target < mid_val {
                right = mid;
            } else {
                return Some(mid);
            }

            size = right - left;
        }
        None
    }
}

const SERIALIZED_BLOCK_METADATA_SIZE: usize = 4;

fn deserialize_sparse_codec_block(data: &[u8]) -> Vec<SparseCodecBlock> {
    // The number of vals so far
    let mut offset = 0;
    let mut sparse_codec_blocks = Vec::new();
    let num_blocks = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);
    let block_data_index_start =
        data.len() - 2 - num_blocks as usize * SERIALIZED_BLOCK_METADATA_SIZE;
    for block_num in 0..num_blocks as usize {
        let block_data_index = block_data_index_start + SERIALIZED_BLOCK_METADATA_SIZE * block_num;
        let block_idx = u16::from_le_bytes([data[block_data_index], data[block_data_index + 1]]);
        let num_vals = u16::from_le_bytes([data[block_data_index + 2], data[block_data_index + 3]]);
        while block_idx != sparse_codec_blocks.len() as u16 {
            // fill up empty blocks
            sparse_codec_blocks.push(SparseCodecBlock::empty_block(offset));
        }
        let block = SparseCodecBlock { num_vals, offset };
        sparse_codec_blocks.push(block);
        offset += num_vals as u32;
    }
    sparse_codec_blocks.push(SparseCodecBlock::empty_block(offset));
    sparse_codec_blocks
}

const ELEMENTS_PER_BLOCK: u32 = u16::MAX as u32;

/// Splits a idx into block index and value in the block
fn split_in_block_idx_and_val_in_block(idx: u32) -> (u16, u16) {
    let val_in_block = (idx % u16::MAX as u32) as u16;
    let block_idx = (idx / u16::MAX as u32) as u16;
    (block_idx, val_in_block)
}

impl SparseCodec {
    /// Open the SparseCodec from OwnedBytes
    pub fn open(data: OwnedBytes) -> Self {
        let blocks = deserialize_sparse_codec_block(&data);
        Self { data, blocks }
    }

    #[inline]
    /// Check if value at position is not null.
    pub fn exists(&self, idx: u32) -> bool {
        let (block_idx, val_in_block) = split_in_block_idx_and_val_in_block(idx);
        // There may be trailing nulls without data, those are not stored as blocks. It would be
        // possible to create empty blocks, but for that we would need to serialize the number of
        // values or pass them when opening

        if let Some(block) = self.blocks.get(block_idx as usize) {
            block.binary_search(&self.data, val_in_block).is_some()
        } else {
            false
        }
    }

    /// Return the number of non-null values in an index
    pub fn num_non_null_vals(&self) -> u32 {
        self.blocks.last().map(|block| block.offset).unwrap_or(0)
    }

    #[inline]
    /// Translate from the original index to the codec index.
    pub fn translate_to_codec_idx(&self, idx: u32) -> Option<u32> {
        let (block_idx, val_in_block) = split_in_block_idx_and_val_in_block(idx);
        let block = &self.blocks.get(block_idx as usize)?;

        let pos_in_block = block.binary_search(&self.data, val_in_block);
        pos_in_block.map(|pos_in_block: u16| block.offset + pos_in_block as u32)
    }

    fn find_block(&self, dense_idx: u32, mut block_pos: u32) -> u32 {
        loop {
            let offset = self.blocks[block_pos as usize].offset;
            if offset > dense_idx {
                return block_pos - 1;
            }
            block_pos += 1;
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
        iter.map(move |codec_idx| {
            // update block_pos to limit search scope
            block_pos = self.find_block(codec_idx, block_pos);
            let block_doc_idx_start = block_pos * ELEMENTS_PER_BLOCK;
            let index_block = &self.blocks[block_pos as usize];
            let idx_in_block = codec_idx - index_block.offset;
            index_block.value_at_idx(&self.data, idx_in_block as u16) as u32 + block_doc_idx_start
        })
    }
}

/// Iterator over positions of set values.
pub fn serialize_sparse_codec(
    mut iter: impl Iterator<Item = u32>,
    mut out: impl Write,
) -> io::Result<()> {
    let mut block_metadata: Vec<(u16, u16)> = Vec::new();
    if let Some(idx) = iter.next() {
        let (block_idx, val_in_block) = split_in_block_idx_and_val_in_block(idx);
        block_metadata.push((block_idx, 1));
        out.write_all(val_in_block.to_le_bytes().as_ref())?;
    }
    for idx in iter {
        let (block_idx, val_in_block) = split_in_block_idx_and_val_in_block(idx);
        if block_metadata[block_metadata.len() - 1].0 == block_idx {
            let last_idx_metadata = block_metadata.len() - 1;
            block_metadata[last_idx_metadata].1 += 1;
        } else {
            block_metadata.push((block_idx, 1));
        }
        out.write_all(val_in_block.to_le_bytes().as_ref())?;
    }
    for block in &block_metadata {
        out.write_all(block.0.to_le_bytes().as_ref())?;
        out.write_all(block.1.to_le_bytes().as_ref())?;
    }
    out.write_all((block_metadata.len() as u16).to_le_bytes().as_ref())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use proptest::prelude::{any, prop, *};
    use proptest::strategy::Strategy;
    use proptest::{prop_oneof, proptest};

    use super::*;

    fn random_bitvec() -> BoxedStrategy<Vec<bool>> {
        prop_oneof![
            1 => prop::collection::vec(proptest::bool::weighted(1.0), 0..100),
            1 => prop::collection::vec(proptest::bool::weighted(0.00), 0..(ELEMENTS_PER_BLOCK as usize * 3)), // empty blocks
            1 => prop::collection::vec(proptest::bool::weighted(1.00), 0..(ELEMENTS_PER_BLOCK as usize + 10)), // full block
            1 => prop::collection::vec(proptest::bool::weighted(0.01), 0..100),
            1 => prop::collection::vec(proptest::bool::weighted(0.01), 0..u16::MAX as usize),
            8 => vec![any::<bool>()],
        ]
        .boxed()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
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
    fn sparse_codec_test_one_block_false() {
        let mut iter = vec![false; ELEMENTS_PER_BLOCK as usize];
        iter.push(true);
        test_null_index(iter);
    }

    #[test]
    fn sparse_codec_test_one_block_true() {
        let mut iter = vec![true; ELEMENTS_PER_BLOCK as usize];
        iter.push(true);
        test_null_index(iter);
    }

    fn test_null_index(data: Vec<bool>) {
        let mut out = vec![];

        serialize_sparse_codec(
            data.iter()
                .cloned()
                .enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();
        let null_index = SparseCodec::open(OwnedBytes::new(out));

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

        let step_size = (orig_idx_with_value.len() / 100).max(1);
        for (dense_idx, orig_idx) in orig_idx_with_value.iter().enumerate().step_by(step_size) {
            assert_eq!(
                null_index.translate_to_codec_idx(*orig_idx),
                Some(dense_idx as u32)
            );
        }

        // 100 samples
        let step_size = (data.len() / 100).max(1);
        for (pos, value) in data.iter().enumerate().step_by(step_size) {
            assert_eq!(null_index.exists(pos as u32), *value);
        }
    }

    #[test]
    fn sparse_codec_test_translation() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_sparse_codec(
            iter.enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();
        let null_index = SparseCodec::open(OwnedBytes::new(out));

        assert_eq!(
            null_index
                .translate_codec_idx_to_original_idx(0..2)
                .collect_vec(),
            vec![0, 2]
        );
    }

    #[test]
    fn sparse_codec_translate() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_sparse_codec(
            iter.enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();
        let null_index = SparseCodec::open(OwnedBytes::new(out));
        assert_eq!(null_index.translate_to_codec_idx(0), Some(0));
        assert_eq!(null_index.translate_to_codec_idx(2), Some(1));
    }

    #[test]
    fn sparse_codec_test_small() {
        let mut out = vec![];

        let iter = ([true, false, true, false]).iter().cloned();
        serialize_sparse_codec(
            iter.enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();
        let null_index = SparseCodec::open(OwnedBytes::new(out));
        assert!(null_index.exists(0));
        assert!(!null_index.exists(1));
        assert!(null_index.exists(2));
        assert!(!null_index.exists(3));
    }

    #[test]
    fn sparse_codec_test_large() {
        let mut docs = vec![];
        docs.extend((0..ELEMENTS_PER_BLOCK).map(|_idx| false));
        docs.extend((0..=1).map(|_idx| true));

        let iter = docs.iter().cloned();
        let mut out = vec![];
        serialize_sparse_codec(
            iter.enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();
        let null_index = SparseCodec::open(OwnedBytes::new(out));
        assert!(!null_index.exists(0));
        assert!(!null_index.exists(100));
        assert!(!null_index.exists(ELEMENTS_PER_BLOCK - 1));
        assert!(null_index.exists(ELEMENTS_PER_BLOCK));
        assert!(null_index.exists(ELEMENTS_PER_BLOCK + 1));
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;

    use super::*;

    const TOTAL_NUM_VALUES: u32 = 1_000_000;
    fn gen_bools(fill_ratio: f64) -> SparseCodec {
        let mut out = Vec::new();
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        serialize_sparse_codec(
            (0..TOTAL_NUM_VALUES)
                .map(|_| rng.gen_bool(fill_ratio))
                .enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
            &mut out,
        )
        .unwrap();

        let codec = SparseCodec::open(OwnedBytes::new(out));
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

    fn walk_over_data(codec: &SparseCodec, max_step_size: u32) -> Option<u32> {
        walk_over_data_from_positions(
            codec,
            random_range_iterator(0, TOTAL_NUM_VALUES, max_step_size),
        )
    }

    fn walk_over_data_from_positions(
        codec: &SparseCodec,
        positions: impl Iterator<Item = u32>,
    ) -> Option<u32> {
        let mut dense_idx: Option<u32> = None;
        for idx in positions {
            dense_idx = dense_idx.or(codec.translate_to_codec_idx(idx));
        }
        dense_idx
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_1percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.01f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_5percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.05f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_full_scan_10percent(bench: &mut Bencher) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_full_scan_90percent(bench: &mut Bencher) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_full_scan_1percent(bench: &mut Bencher) {
        let codec = gen_bools(0.01f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_10percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_sparse_codec_translate_orig_to_sparse_90percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_sparse_codec_translate_sparse_to_orig_1percent_filled_random_stride_big_step(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.01f64);
        let num_vals = codec.num_non_null_vals();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(random_range_iterator(0, num_vals, 50_000))
                .last()
        });
    }

    #[bench]
    fn bench_sparse_codec_translate_sparse_to_orig_1percent_filled_random_stride(
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(0.01f64);
        let num_vals = codec.num_non_null_vals();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(random_range_iterator(0, num_vals, 100))
                .last()
        });
    }

    #[bench]
    fn bench_sparse_codec_translate_sparse_to_orig_1percent_filled_full_scan(bench: &mut Bencher) {
        let codec = gen_bools(0.01f64);
        let num_vals = codec.num_non_null_vals();
        bench.iter(|| {
            codec
                .translate_codec_idx_to_original_idx(0..num_vals)
                .last()
        });
    }
}
