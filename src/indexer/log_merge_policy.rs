use std::cmp;

use itertools::Itertools;

use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::index::SegmentMeta;

const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
const DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE: usize = 8;
const DEFAULT_TARGET_SEGMENT_SIZE: usize = 10_000_000;
// The default value of 1 means that deletes are not taken in account when
// identifying merge candidates. This is not a very sensible default: it was
// set like that for backward compatibility and might change in the near future.
const DEFAULT_DEL_DOCS_RATIO_BEFORE_MERGE: f32 = 1.0f32;

/// `LogMergePolicy` tries to merge segments that have a similar number of
/// documents.
#[derive(Debug, Clone)]
pub struct LogMergePolicy {
    min_num_segments: usize,
    target_segment_size: usize,
    min_layer_size: u32,
    level_log_size: f64,
    del_docs_ratio_before_merge: f32,
}

impl LogMergePolicy {
    fn clip_min_size(&self, size: u32) -> u32 {
        cmp::max(self.min_layer_size, size)
    }

    /// Set the minimum number of segments that may be merged together.
    pub fn set_min_num_segments(&mut self, min_num_segments: usize) {
        self.min_num_segments = min_num_segments;
    }

    /// Set the target number of documents to have in a segment, a segment can have up to
    /// `(target_segment_size * 2) - 2` documents, but the policy will try to keep them as close as
    /// possible to `target_segment_size`
    pub fn set_target_segment_size(&mut self, max_docs_merge_size: usize) {
        self.target_segment_size = max_docs_merge_size;
    }

    /// Set the minimum segment size under which all segment belong
    /// to the same level.
    pub fn set_min_layer_size(&mut self, min_layer_size: u32) {
        self.min_layer_size = min_layer_size;
    }

    /// Set the ratio between two consecutive levels.
    ///
    /// Segments are grouped in levels according to their sizes.
    /// These levels are defined as intervals of exponentially growing sizes.
    /// level_log_size define the factor by which one should multiply the limit
    /// to reach a level, in order to get the limit to reach the following
    /// level.
    pub fn set_level_log_size(&mut self, level_log_size: f64) {
        self.level_log_size = level_log_size;
    }

    /// Set the ratio of deleted documents in a segment to tolerate.
    ///
    /// If it is exceeded by any segment at a log level, a merge
    /// will be triggered for that level.
    ///
    /// If there is a single segment at a level, we effectively end up expunging
    /// deleted documents from it.
    ///
    /// # Panics
    ///
    /// Panics if del_docs_ratio_before_merge is not within (0..1].
    pub fn set_del_docs_ratio_before_merge(&mut self, del_docs_ratio_before_merge: f32) {
        assert!(del_docs_ratio_before_merge <= 1.0f32);
        assert!(del_docs_ratio_before_merge > 0f32);
        self.del_docs_ratio_before_merge = del_docs_ratio_before_merge;
    }

    fn has_segment_above_deletes_threshold(&self, level: &[&SegmentMeta]) -> bool {
        level
            .iter()
            .any(|segment| deletes_ratio(segment) > self.del_docs_ratio_before_merge)
    }
}

fn deletes_ratio(segment: &SegmentMeta) -> f32 {
    if segment.max_doc() == 0 {
        return 0f32;
    }
    segment.num_deleted_docs() as f32 / segment.max_doc() as f32
}

impl MergePolicy for LogMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        // Get segments that are small enough to be merged and sort them by size in descending
        // order.
        let size_sorted_segments = segments
            .iter()
            .filter(|seg| (seg.num_docs() as usize) < self.target_segment_size)
            .sorted_by_key(|seg| std::cmp::Reverse(seg.max_doc()))
            .collect_vec();

        // If there are no small enough segments, return an empty vector.
        if size_sorted_segments.is_empty() {
            return Vec::new();
        }

        let mut candidates = Vec::new();
        let mut levels = Vec::new();
        let mut unmerged_docs = 0usize;
        let mut current_max_log_size = f64::MAX;
        for (_, merge_group) in &size_sorted_segments.into_iter().chunk_by(|segment| {
            let segment_log_size = f64::from(self.clip_min_size(segment.num_docs())).log2();
            if segment_log_size < (current_max_log_size - self.level_log_size) {
                // update current_max_log_size to create a new group
                current_max_log_size = segment_log_size;
            }
            // accumulate the number of documents
            unmerged_docs += segment.num_docs() as usize;
            // return current_max_log_size to be grouped to the current group
            current_max_log_size
        }) {
            levels.push(merge_group.collect::<Vec<&SegmentMeta>>());
        }

        // If the total number of unmerged documents is large enough to reach the target size,
        // then start collecting segments in ascending size until we reach the target size.
        if unmerged_docs >= self.target_segment_size {
            let mut batch_docs = 0usize;
            let mut batch = Vec::new();
            // Pop segments segments from levels, smallest first due to sort at start
            while let Some(segments) = levels.pop() {
                for s in segments {
                    batch_docs += s.num_docs() as usize;
                    batch.push(s);

                    // If the current batch has enough documents to be merged, create a merge
                    // candidate and push it to candidates
                    if batch_docs >= self.target_segment_size {
                        unmerged_docs -= batch_docs;
                        batch_docs = 0;
                        candidates.push(MergeCandidate(
                            // drain to reuse the buffer
                            batch.drain(..).map(|seg| seg.id()).collect(),
                        ));
                    }
                }

                // If there are no longer enough documents to create a skip merge, break the loop
                // unmerged_docs is only updated when a batch is created so this won't trigger
                // before we have enough docs collected
                if unmerged_docs <= self.target_segment_size {
                    break;
                }
            }
            // If there are any remaining segments in the batch, push them as a level to be
            // processed by the standard merge policy
            if !batch.is_empty() {
                levels.push(batch);
            }
        }

        levels
            .into_iter()
            .filter(|level| {
                level.len() >= self.min_num_segments
                    || self.has_segment_above_deletes_threshold(level)
            })
            .for_each(|level| {
                candidates.push(MergeCandidate(
                    level.into_iter().map(|seg| seg.id()).collect(),
                ))
            });

        candidates
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy {
            min_num_segments: DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE,
            target_segment_size: DEFAULT_TARGET_SEGMENT_SIZE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
            del_docs_ratio_before_merge: DEFAULT_DEL_DOCS_RATIO_BEFORE_MERGE,
        }
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;
    use crate::index::{SegmentId, SegmentMetaInventory};
    use crate::schema;
    use crate::schema::INDEXED;

    static INVENTORY: Lazy<SegmentMetaInventory> = Lazy::new(SegmentMetaInventory::default);

    use crate::Index;

    #[test]
    fn create_index_test_max_merge_issue_1035() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intval", INDEXED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        {
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_num_segments(1);
            log_merge_policy.set_target_segment_size(1);
            log_merge_policy.set_min_layer_size(0);

            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(log_merge_policy));

            // after every commit the merge checker is started, it will merge only segments with 1
            // element in it because of the max_docs_before_merge.
            index_writer.add_document(doc!(int_field=>1_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>2_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>3_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>4_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>5_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>6_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>7_u64))?;
            index_writer.commit()?;

            index_writer.add_document(doc!(int_field=>8_u64))?;
            index_writer.commit()?;
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
        Ok(())
    }

    fn test_merge_policy() -> LogMergePolicy {
        let mut log_merge_policy = LogMergePolicy::default();
        log_merge_policy.set_min_num_segments(3);
        log_merge_policy.set_target_segment_size(100_000);
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
        // All segments at or above target size, so we expect nothing
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_skip_merge_large_segments() {
        // Test that we skip log merges if there are enough unmerged documents to reach the target
        // size
        let test_input = vec![
            create_random_segment_meta(50_000),
            create_random_segment_meta(50_000),
            create_random_segment_meta(49_999),
            create_random_segment_meta(49_999),
            create_random_segment_meta(49_999),
            create_random_segment_meta(49_999),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);

        assert_eq!(result_list.len(), 2);
        // First result should be the first 2 segments being merged into a single 100k segment
        assert_eq!(result_list[0].0.len(), 2);
        assert_eq!(result_list[0].0[0], test_input[0].id());
        assert_eq!(result_list[0].0[1], test_input[1].id());

        // Second results should be the next 3 segments, excluding the final segment as it will have
        // already hit the target
        assert_eq!(result_list[1].0.len(), 3);
        assert_eq!(result_list[1].0[0], test_input[2].id());
        assert_eq!(result_list[1].0[1], test_input[3].id());
        assert_eq!(result_list[1].0[2], test_input[4].id());
    }

    #[test]
    fn test_skip_merge_small_segments() {
        // Test that we skip log merges if there are enough unmerged documents to reach the target
        // size
        let test_input = vec![
            create_random_segment_meta(75_000),
            create_random_segment_meta(75_000),
            create_random_segment_meta(5_000),
            create_random_segment_meta(5_000),
            create_random_segment_meta(5_000),
            create_random_segment_meta(5_000),
            create_random_segment_meta(5_000),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);

        // Should have a single merge with all of the small segments and only one of the large
        // segments
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 6);
        assert_eq!(result_list[0].0[0], test_input[2].id());
        assert_eq!(result_list[0].0[1], test_input[3].id());
        assert_eq!(result_list[0].0[2], test_input[4].id());
        assert_eq!(result_list[0].0[3], test_input[5].id());
        assert_eq!(result_list[0].0[4], test_input[6].id());
        assert!(
            result_list[0].0[5] == test_input[0].id() || result_list[0].0[5] == test_input[1].id()
        );
    }

    #[test]
    fn test_merge_single_segment_with_deletes_below_threshold() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![create_random_segment_meta(40_000).with_delete_meta(10_000, 1)];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert!(merge_candidates.is_empty());
    }

    #[test]
    fn test_merge_single_segment_with_deletes_above_threshold() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![create_random_segment_meta(40_000).with_delete_meta(10_001, 1)];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
    }

    #[test]
    fn test_merge_segments_with_deletes_above_threshold_all_in_level() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![
            create_random_segment_meta(40_000).with_delete_meta(10_001, 1),
            create_random_segment_meta(40_000),
        ];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
        assert_eq!(merge_candidates[0].0.len(), 2);
    }

    #[test]
    fn test_merge_segments_with_deletes_above_threshold_different_level_not_involved() {
        let mut test_merge_policy = test_merge_policy();
        test_merge_policy.set_del_docs_ratio_before_merge(0.25f32);
        let test_input = vec![
            create_random_segment_meta(100),
            create_random_segment_meta(40_000).with_delete_meta(10_001, 1),
        ];
        let merge_candidates = test_merge_policy.compute_merge_candidates(&test_input);
        assert_eq!(merge_candidates.len(), 1);
        assert_eq!(merge_candidates[0].0.len(), 1);
        assert_eq!(merge_candidates[0].0[0], test_input[1].id());
    }
}
