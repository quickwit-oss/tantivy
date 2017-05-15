extern crate itertools;
use super::merge_policy::{MergePolicy, MergeCandidate};
use core::SegmentMeta;
use std::cmp;
use std::f64;

const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
const DEFAULT_MIN_MERGE_SIZE: usize = 8;


/// LogMergePolicy tries tries to merge segments that have a similar number of
/// documents.
#[derive(Debug, Clone)]
pub struct LogMergePolicy {
    min_merge_size: usize,
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
        if segments.is_empty() {
            return Vec::new();
        }

        let mut size_sorted_tuples = segments
            .iter()
            .map(|x| x.num_docs())
            .enumerate()
            .collect::<Vec<(usize, u32)>>();

        size_sorted_tuples.sort_by(|x, y| y.cmp(x));

        let size_sorted_log_tuples: Vec<_> = size_sorted_tuples
            .into_iter()
            .map(|(ind, num_docs)| (ind, (self.clip_min_size(num_docs) as f64).log2()))
            .collect();

        let (first_ind, first_score) = size_sorted_log_tuples[0];
        let mut current_max_log_size = first_score;
        let mut levels = vec![vec![first_ind]];
        for &(ind, score) in (&size_sorted_log_tuples).iter().skip(1) {
            if score < (current_max_log_size - self.level_log_size) {
                current_max_log_size = score;
                levels.push(Vec::new());
            }
            levels.last_mut().unwrap().push(ind);
        }

        levels
            .iter()
            .filter(|level| level.len() >= self.min_merge_size)
            .map(|ind_vec| MergeCandidate(ind_vec.iter().map(|&ind| segments[ind].id()).collect()))
            .collect()
    }

    fn box_clone(&self) -> Box<MergePolicy> {
        box self.clone()
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy {
            min_merge_size: DEFAULT_MIN_MERGE_SIZE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::merge_policy::MergePolicy;
    use core::{SegmentMeta, SegmentId};

    fn test_merge_policy() -> LogMergePolicy {
        let mut log_merge_policy = LogMergePolicy::default();
        log_merge_policy.set_min_merge_size(3);
        log_merge_policy.set_min_layer_size(2);
        log_merge_policy
    }

    #[test]
    fn test_log_merge_policy_empty() {
        let y = Vec::new();
        let result_list = test_merge_policy().compute_merge_candidates(&y);
        assert!(result_list.is_empty());
    }

    fn seg_meta(num_docs: u32) -> SegmentMeta {
        let mut segment_metas = SegmentMeta::new(SegmentId::generate_random());
        segment_metas.set_max_doc(num_docs);
        segment_metas
    }

    #[test]
    fn test_log_merge_policy_pair() {
        let test_input = vec![seg_meta(10), seg_meta(10), seg_meta(10)];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }

    #[test]
    fn test_log_merge_policy_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![seg_meta(10),
                              seg_meta(10),
                              seg_meta(10),
                              seg_meta(1000),
                              seg_meta(1000),
                              seg_meta(1000)];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }

    #[test]
    fn test_log_merge_policy_within_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![seg_meta(10),
                              seg_meta(11),
                              seg_meta(12),
                              seg_meta(1000),
                              seg_meta(1000),
                              seg_meta(1000)];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
    }
    #[test]
    fn test_log_merge_policy_small_segments() {
        // multiple levels all get merged correctly
        let test_input = vec![seg_meta(1),
                              seg_meta(1),
                              seg_meta(1),
                              seg_meta(2),
                              seg_meta(2),
                              seg_meta(2)];
        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
    }
}
