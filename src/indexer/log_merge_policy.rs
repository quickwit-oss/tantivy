use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::core::SegmentMeta;
use std::cmp;
use std::f64;

const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
const DEFAULT_MIN_MERGE_SIZE: usize = 8;
const DEFAULT_MAX_MERGE_SIZE: usize = 10_000_000;

/// `LogMergePolicy` tries to merge segments that have a similar number of
/// documents.
#[derive(Debug, Clone)]
pub struct LogMergePolicy {
    min_merge_size: usize,
    max_merge_size: usize,
    min_layer_size: u32,
    level_log_size: f64,
}

impl LogMergePolicy {
    fn clip_min_size(&self, size: u32) -> u32 {
        cmp::max(self.min_layer_size, size)
    }

    /// Set the minimum number of segment that may be merge together.
    pub fn set_min_merge_size(&mut self, min_merge_size: usize) {
        self.min_merge_size = min_merge_size;
    }

    /// Set the maximum number docs in a segment for it to be considered for
    /// merging.
    pub fn set_max_merge_size(&mut self, max_merge_size: usize) {
        self.max_merge_size = max_merge_size;
    }

    /// Set the minimum segment size under which all segment belong
    /// to the same level.
    pub fn set_min_layer_size(&mut self, min_layer_size: u32) {
        self.min_layer_size = min_layer_size;
    }

    /// Set the ratio between two consecutive levels.
    ///
    /// Segment are group in levels according to their sizes.
    /// These levels are defined as intervals of exponentially growing sizes.
    /// level_log_size define the factor by which one should multiply the limit
    /// to reach a level, in order to get the limit to reach the following
    /// level.
    pub fn set_level_log_size(&mut self, level_log_size: f64) {
        self.level_log_size = level_log_size;
    }
}

impl MergePolicy for LogMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let mut size_sorted_segments = segments
            .iter()
            .filter(|s| s.num_docs() <= (self.max_merge_size as u32))
            .collect::<Vec<&SegmentMeta>>();

        if size_sorted_segments.len() <= 1 {
            return vec![];
        }
        size_sorted_segments.sort_by_key(|seg| seg.num_docs());

        let sorted_segments_with_log_size: Vec<_> = size_sorted_segments
            .into_iter()
            .map(|seg| (seg, f64::from(self.clip_min_size(seg.num_docs())).log2()))
            .collect();

        if let Some(&(first_segment, log_size)) = sorted_segments_with_log_size.first() {
            let mut current_max_log_size = log_size;
            let mut levels = vec![vec![first_segment]];
            for &(segment, segment_log_size) in (&sorted_segments_with_log_size).iter().skip(1) {
                if segment_log_size < (current_max_log_size - self.level_log_size) {
                    current_max_log_size = segment_log_size;
                    levels.push(Vec::new());
                }
                levels.last_mut().unwrap().push(segment);
            }
            levels
                .iter()
                .filter(|level| level.len() >= self.min_merge_size)
                .map(|segments| MergeCandidate(segments.iter().map(|&seg| seg.id()).collect()))
                .collect()
        } else {
            return vec![];
        }
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy {
            min_merge_size: DEFAULT_MIN_MERGE_SIZE,
            max_merge_size: DEFAULT_MAX_MERGE_SIZE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{SegmentId, SegmentMeta, SegmentMetaInventory},
        schema,
    };
    use crate::{indexer::merge_policy::MergePolicy, schema::INDEXED};
    use once_cell::sync::Lazy;

    static INVENTORY: Lazy<SegmentMetaInventory> = Lazy::new(SegmentMetaInventory::default);

    use crate::Index;

    #[test]
    fn create_index_test_max_merge_issue_1035() {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intval", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        {
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_merge_size(1);
            log_merge_policy.set_max_merge_size(1);
            log_merge_policy.set_min_layer_size(0);

            let mut index_writer = index.writer_for_tests().unwrap();
            index_writer.set_merge_policy(Box::new(log_merge_policy));

            // after every commit the merge checker is started, it will merge only segments with 1
            // element in it because of the max_merge_size.
            index_writer.add_document(doc!(int_field=>1_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>2_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>3_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>4_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>5_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>6_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>7_u64));
            assert!(index_writer.commit().is_ok());

            index_writer.add_document(doc!(int_field=>8_u64));
            assert!(index_writer.commit().is_ok());
        }

        let _segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_readers = searcher.segment_readers();
        for segment in segment_readers {
            if segment.num_docs() > 2 {
                panic!("segment can't have more than two segments");
            } // don't know how to wait for the merge, then it could be a simple eq
        }
    }

    fn test_merge_policy() -> LogMergePolicy {
        let mut log_merge_policy = LogMergePolicy::default();
        log_merge_policy.set_min_merge_size(3);
        log_merge_policy.set_max_merge_size(100_000);
        log_merge_policy.set_min_layer_size(2);
        log_merge_policy
    }

    #[test]
    fn test_log_merge_policy_empty() {
        let y = Vec::new();
        let result_list = test_merge_policy().compute_merge_candidates(&y);
        assert!(result_list.is_empty());
    }

    fn create_random_segment_meta(num_docs: u32) -> SegmentMeta {
        INVENTORY.new_segment_meta(SegmentId::generate_random(), num_docs)
    }

    #[test]
    fn test_log_merge_policy_pair() {
        let test_input = vec![
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }

    #[test]
    fn test_log_merge_policy_levels() {
        // multiple levels all get merged correctly
        // 2 MergeCandidates expected:
        // * one with the 6 * 10-docs segments
        // * one with the 3 * 1000-docs segments
        // no MergeCandidate expected for the 2 * 10_000-docs segments as min_merge_size=3
        let test_input = vec![
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(1_000),
            create_random_segment_meta(1_000),
            create_random_segment_meta(1_000),
            create_random_segment_meta(10_000),
            create_random_segment_meta(10_000),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
            create_random_segment_meta(10),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }

    #[test]
    fn test_log_merge_policy_within_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![
            create_random_segment_meta(10),   // log2(10) = ~3.32 (> 3.58 - 0.75)
            create_random_segment_meta(11),   // log2(11) = ~3.46
            create_random_segment_meta(12),   // log2(12) = ~3.58
            create_random_segment_meta(800),  // log2(800) = ~9.64 (> 9.97 - 0.75)
            create_random_segment_meta(1000), // log2(1000) = ~9.97
            create_random_segment_meta(1000),
        ]; // log2(1000) = ~9.97
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }

    #[test]
    fn test_log_merge_policy_small_segments() {
        // segments under min_layer_size are merged together
        let test_input = vec![
            create_random_segment_meta(1),
            create_random_segment_meta(1),
            create_random_segment_meta(1),
            create_random_segment_meta(2),
            create_random_segment_meta(2),
            create_random_segment_meta(2),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }

    #[test]
    fn test_log_merge_policy_all_segments_too_large_to_merge() {
        let eight_large_segments: Vec<SegmentMeta> =
            std::iter::repeat_with(|| create_random_segment_meta(100_001))
                .take(8)
                .collect();
        assert!(test_merge_policy()
            .compute_merge_candidates(&eight_large_segments)
            .is_empty());
    }

    #[test]
    fn test_large_merge_segments() {
        let test_input = vec![
            create_random_segment_meta(1_000_000),
            create_random_segment_meta(100_001),
            create_random_segment_meta(100_000),
            create_random_segment_meta(1_000_001),
            create_random_segment_meta(100_000),
            create_random_segment_meta(100_000),
            create_random_segment_meta(1_500_000),
        ];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        // Do not include large segments
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 3);

        // Making sure merge policy points to the correct index of the original input
        assert_eq!(result_list[0].0[0], test_input[2].id());
        assert_eq!(result_list[0].0[1], test_input[4].id());
        assert_eq!(result_list[0].0[2], test_input[5].id());
    }
}
