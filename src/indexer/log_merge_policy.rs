extern crate itertools;
use super::merge_policy::{MergePolicy, MergeCandidate};
use core::SegmentMeta;

pub struct LogMergePolicy;
use std::f64;

const LEVEL_LOG_SIZE: f64 = 0.75;

impl MergePolicy for LogMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        if segments.is_empty() {
            return Vec::new();
        }
        let mut size_sorted_tuples = segments.iter()
            .map(|x| x.num_docs)
            .enumerate()
            .collect::<Vec<(usize, u32)>>();

        size_sorted_tuples.sort_by(|x,y| y.cmp(x));

        let size_sorted_log_tuples: Vec<_> = size_sorted_tuples.iter()
            .map(|x| (x.0, (x.1 as f64).log2()))
            .collect();

        let (first_ind, first_score) = size_sorted_log_tuples[0];
        let mut current_max_log_size = first_score;
        let mut levels = Vec::new();
        levels.push(Vec::new());
        levels.last_mut().unwrap().push(first_ind);
        for &(ind, score) in (&size_sorted_log_tuples).iter().skip(1) {
            if score < (current_max_log_size - LEVEL_LOG_SIZE) {
                current_max_log_size = score;
                levels.push(Vec::new());
            }
            levels.last_mut().unwrap().push(ind);
        }

        let result = levels.iter()
            .map(|ind_vec| {
                MergeCandidate(ind_vec.iter()
                    .map(|&ind| segments[ind].segment_id)
                    .collect())
            })
            .collect();

        result
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::merge_policy::MergePolicy;
    use core::{SegmentMeta, SegmentId};

    #[test]
    fn test_log_merge_policy_empty() {
        let y = Vec::new();
        let result_list = LogMergePolicy::default().compute_merge_candidates(&y);
        assert!(result_list.len() == 0);
    }

    #[test]
    fn test_log_merge_policy_pair() {
        let test_input = vec![SegmentMeta::new(SegmentId::generate_random(), 10),
                              SegmentMeta::new(SegmentId::generate_random(), 10)];
        let result_list = LogMergePolicy::default().compute_merge_candidates(&test_input);
        assert!(result_list.len() == 1);
    }

    #[test]
    fn test_log_merge_policy_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![SegmentMeta::new(SegmentId::generate_random(), 10),
                              SegmentMeta::new(SegmentId::generate_random(), 10),
                              SegmentMeta::new(SegmentId::generate_random(), 1000),
                              SegmentMeta::new(SegmentId::generate_random(), 1000)];
        let result_list = LogMergePolicy::default().compute_merge_candidates(&test_input);
        assert!(result_list.len() == 2);
    }

    #[test]
    fn test_log_merge_policy_within_levels() {
        // multiple levels all get merged correctly
        let test_input = vec![SegmentMeta::new(SegmentId::generate_random(), 10),
                              SegmentMeta::new(SegmentId::generate_random(), 11),
                              SegmentMeta::new(SegmentId::generate_random(), 12),
                              SegmentMeta::new(SegmentId::generate_random(), 1000),
                              SegmentMeta::new(SegmentId::generate_random(), 1000)];
        let result_list = LogMergePolicy::default().compute_merge_candidates(&test_input);
        assert!(result_list.len() == 2);
    }
}
