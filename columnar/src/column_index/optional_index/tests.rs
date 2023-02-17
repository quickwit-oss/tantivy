use proptest::prelude::{any, prop, *};
use proptest::strategy::Strategy;
use proptest::{prop_oneof, proptest};

use super::*;

#[test]
fn test_dense_block_threshold() {
    assert_eq!(super::DENSE_BLOCK_THRESHOLD, 5_120);
}

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
    #![proptest_config(ProptestConfig::with_cases(50))]
    #[test]
    fn test_with_random_bitvecs(bitvec1 in random_bitvec(), bitvec2 in random_bitvec(), bitvec3 in random_bitvec()) {
        let mut bitvec = Vec::new();
        bitvec.extend_from_slice(&bitvec1);
        bitvec.extend_from_slice(&bitvec2);
        bitvec.extend_from_slice(&bitvec3);
        test_null_index(&bitvec[..]);
    }
}

#[test]
fn test_with_random_sets_simple() {
    let vals = 10..BLOCK_SIZE * 2;
    let mut out: Vec<u8> = Vec::new();
    serialize_optional_index(&vals, 100, &mut out).unwrap();
    let null_index = open_optional_index(OwnedBytes::new(out)).unwrap();
    let ranks: Vec<u32> = (65_472u32..65_473u32).collect();
    let els: Vec<u32> = ranks.iter().copied().map(|rank| rank + 10).collect();
    let mut select_cursor = null_index.select_cursor();
    for (rank, el) in ranks.iter().copied().zip(els.iter().copied()) {
        assert_eq!(select_cursor.select(rank), el);
    }
}

#[test]
fn test_optional_index_trailing_empty_blocks() {
    test_null_index(&[false]);
}

#[test]
fn test_optional_index_one_block_false() {
    let mut iter = vec![false; ELEMENTS_PER_BLOCK as usize];
    iter.push(true);
    test_null_index(&iter[..]);
}

#[test]
fn test_optional_index_one_block_true() {
    let mut iter = vec![true; ELEMENTS_PER_BLOCK as usize];
    iter.push(true);
    test_null_index(&iter[..]);
}

impl<'a> Iterable<RowId> for &'a [bool] {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + 'a> {
        Box::new(
            self.iter()
                .cloned()
                .enumerate()
                .filter(|(_pos, val)| *val)
                .map(|(pos, _val)| pos as u32),
        )
    }
}

fn test_null_index(data: &[bool]) {
    let mut out: Vec<u8> = Vec::new();
    serialize_optional_index(&data, data.len() as RowId, &mut out).unwrap();
    let null_index = open_optional_index(OwnedBytes::new(out)).unwrap();
    let orig_idx_with_value: Vec<u32> = data
        .iter()
        .enumerate()
        .filter(|(_pos, val)| **val)
        .map(|(pos, _val)| pos as u32)
        .collect();
    let mut select_iter = null_index.select_cursor();
    for i in 0..orig_idx_with_value.len() {
        assert_eq!(select_iter.select(i as u32), orig_idx_with_value[i]);
    }

    let step_size = (orig_idx_with_value.len() / 100).max(1);
    for (dense_idx, orig_idx) in orig_idx_with_value.iter().enumerate().step_by(step_size) {
        assert_eq!(null_index.rank_if_exists(*orig_idx), Some(dense_idx as u32));
    }

    // 100 samples
    let step_size = (data.len() / 100).max(1);
    for (pos, value) in data.iter().enumerate().step_by(step_size) {
        assert_eq!(null_index.contains(pos as u32), *value);
    }
}

#[test]
fn test_optional_index_test_translation() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    let mut select_cursor = optional_index.select_cursor();
    assert_eq!(select_cursor.select(0), 0);
    assert_eq!(select_cursor.select(1), 2);
}

#[test]
fn test_optional_index_translate() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    assert_eq!(optional_index.rank_if_exists(0), Some(0));
    assert_eq!(optional_index.rank_if_exists(2), Some(1));
}

#[test]
fn test_optional_index_small() {
    let optional_index = OptionalIndex::for_test(4, &[0, 2]);
    assert!(optional_index.contains(0));
    assert!(!optional_index.contains(1));
    assert!(optional_index.contains(2));
    assert!(!optional_index.contains(3));
}

#[test]
fn test_optional_index_large() {
    let row_ids = &[ELEMENTS_PER_BLOCK, ELEMENTS_PER_BLOCK + 1];
    let optional_index = OptionalIndex::for_test(ELEMENTS_PER_BLOCK + 2, row_ids);
    assert!(!optional_index.contains(0));
    assert!(!optional_index.contains(100));
    assert!(!optional_index.contains(ELEMENTS_PER_BLOCK - 1));
    assert!(optional_index.contains(ELEMENTS_PER_BLOCK));
    assert!(optional_index.contains(ELEMENTS_PER_BLOCK + 1));
}

fn test_optional_index_iter_aux(row_ids: &[RowId], num_rows: RowId) {
    let optional_index = OptionalIndex::for_test(num_rows, row_ids);
    assert_eq!(optional_index.num_docs(), num_rows);
    assert!(optional_index.iter_rows().eq(row_ids.iter().copied()));
}

#[test]
fn test_optional_index_iter_empty() {
    test_optional_index_iter_aux(&[], 0u32);
}

fn test_optional_index_rank_aux(row_ids: &[RowId]) {
    let num_rows = row_ids.last().copied().unwrap_or(0u32) + 1;
    let null_index = OptionalIndex::for_test(num_rows, row_ids);
    assert_eq!(null_index.num_docs(), num_rows);
    for (row_id, row_val) in row_ids.iter().copied().enumerate() {
        assert_eq!(null_index.rank(row_val), row_id as u32);
        assert_eq!(null_index.rank_if_exists(row_val), Some(row_id as u32));
        if row_val > 0 && !null_index.contains(&row_val - 1) {
            assert_eq!(null_index.rank(row_val - 1), row_id as u32);
        }
        assert_eq!(null_index.rank(row_val + 1), row_id as u32 + 1);
    }
}

#[test]
fn test_optional_index_rank() {
    test_optional_index_rank_aux(&[1u32]);
    test_optional_index_rank_aux(&[0u32, 1u32]);
    let mut block = Vec::new();
    block.push(3u32);
    block.extend((0..BLOCK_SIZE).map(|i| i + BLOCK_SIZE + 1));
    test_optional_index_rank_aux(&block);
}

#[test]
fn test_optional_index_iter_empty_one() {
    test_optional_index_iter_aux(&[1], 2u32);
    test_optional_index_iter_aux(&[100_000], 200_000u32);
}

#[test]
fn test_optional_index_iter_dense_block() {
    let mut block = Vec::new();
    block.push(3u32);
    block.extend((0..BLOCK_SIZE).map(|i| i + BLOCK_SIZE + 1));
    test_optional_index_iter_aux(&block, 3 * BLOCK_SIZE);
}

#[test]
fn test_optional_index_for_tests() {
    let optional_index = OptionalIndex::for_test(4, &[1, 2]);
    assert!(!optional_index.contains(0));
    assert!(optional_index.contains(1));
    assert!(optional_index.contains(2));
    assert!(!optional_index.contains(3));
    assert_eq!(optional_index.num_docs(), 4);
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::Bencher;

    use super::*;

    const TOTAL_NUM_VALUES: u32 = 1_000_000;
    fn gen_bools(fill_ratio: f64) -> OptionalIndex {
        let mut out = Vec::new();
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        let vals: Vec<RowId> = (0..TOTAL_NUM_VALUES)
            .map(|_| rng.gen_bool(fill_ratio))
            .enumerate()
            .filter(|(pos, val)| *val)
            .map(|(pos, _)| pos as RowId)
            .collect();
        serialize_optional_index(&&vals[..], TOTAL_NUM_VALUES, &mut out).unwrap();
        let codec = open_optional_index(OwnedBytes::new(out)).unwrap();
        codec
    }

    fn random_range_iterator(
        start: u32,
        end: u32,
        avg_step_size: u32,
        avg_deviation: u32,
    ) -> impl Iterator<Item = u32> {
        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
        let mut current = start;
        std::iter::from_fn(move || {
            current += rng.gen_range(avg_step_size - avg_deviation..=avg_step_size + avg_deviation);
            if current >= end {
                None
            } else {
                Some(current)
            }
        })
    }

    fn n_percent_step_iterator(percent: f32, num_values: u32) -> impl Iterator<Item = u32> {
        let ratio = percent as f32 / 100.0;
        let step_size = (1f32 / ratio) as u32;
        let deviation = step_size - 1;
        random_range_iterator(0, num_values, step_size, deviation)
    }

    fn walk_over_data(codec: &OptionalIndex, avg_step_size: u32) -> Option<u32> {
        walk_over_data_from_positions(
            codec,
            random_range_iterator(0, TOTAL_NUM_VALUES, avg_step_size, 0),
        )
    }

    fn walk_over_data_from_positions(
        codec: &OptionalIndex,
        positions: impl Iterator<Item = u32>,
    ) -> Option<u32> {
        let mut dense_idx: Option<u32> = None;
        for idx in positions {
            dense_idx = dense_idx.or(codec.rank_if_exists(idx));
        }
        dense_idx
    }

    #[bench]
    fn bench_translate_orig_to_codec_1percent_filled_10percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.01f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_translate_orig_to_codec_5percent_filled_10percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.05f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_translate_orig_to_codec_5percent_filled_1percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.05f64);
        bench.iter(|| walk_over_data(&codec, 1000));
    }

    #[bench]
    fn bench_translate_orig_to_codec_full_scan_1percent_filled(bench: &mut Bencher) {
        let codec = gen_bools(0.01f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_translate_orig_to_codec_full_scan_10percent_filled(bench: &mut Bencher) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_translate_orig_to_codec_full_scan_90percent_filled(bench: &mut Bencher) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data_from_positions(&codec, 0..TOTAL_NUM_VALUES));
    }

    #[bench]
    fn bench_translate_orig_to_codec_10percent_filled_1percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.1f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_translate_orig_to_codec_50percent_filled_1percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.5f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_translate_orig_to_codec_90percent_filled_1percent_hit(bench: &mut Bencher) {
        let codec = gen_bools(0.9f64);
        bench.iter(|| walk_over_data(&codec, 100));
    }

    #[bench]
    fn bench_translate_codec_to_orig_1percent_filled_0comma005percent_hit(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.01f64, 0.005f32, bench);
    }

    #[bench]
    fn bench_translate_codec_to_orig_10percent_filled_0comma005percent_hit(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.1f64, 0.005f32, bench);
    }

    #[bench]
    fn bench_translate_codec_to_orig_1percent_filled_10percent_hit(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.01f64, 10f32, bench);
    }

    #[bench]
    fn bench_translate_codec_to_orig_1percent_filled_full_scan(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.01f64, 100f32, bench);
    }

    fn bench_translate_codec_to_orig_util(
        percent_filled: f64,
        percent_hit: f32,
        bench: &mut Bencher,
    ) {
        let codec = gen_bools(percent_filled);
        let num_non_nulls = codec.num_non_nulls();
        let idxs: Vec<u32> = if percent_hit == 100.0f32 {
            (0..num_non_nulls).collect()
        } else {
            n_percent_step_iterator(percent_hit, num_non_nulls).collect()
        };
        let mut output = vec![0u32; idxs.len()];
        bench.iter(|| {
            output.copy_from_slice(&idxs[..]);
            codec.select_batch(&mut output);
        });
    }

    #[bench]
    fn bench_translate_codec_to_orig_90percent_filled_0comma005percent_hit(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.9f64, 0.005, bench);
    }

    #[bench]
    fn bench_translate_codec_to_orig_90percent_filled_full_scan(bench: &mut Bencher) {
        bench_translate_codec_to_orig_util(0.9f64, 100.0f32, bench);
    }
}
