extern crate itertools;
use super::merge_policy::{MergePolicy, MergeCandidate};
use core::SegmentMeta;
use self::itertools::Itertools;

pub struct LogMergePolicy;
use std::f64;

const PACK_LEN: usize = 8;
const LEVEL_LOG_SIZE: f64 = 0.75;

impl MergePolicy for LogMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        // what about when segments is empty??
        // take log of size of each segment
        // log2
        let mut size_sorted_tuples = segments.iter()
            .map(|x| {x.num_docs})
            .enumerate()
            .collect::<Vec<(usize, u32)>>();

        size_sorted_tuples.sort_by_key(|x| {x.1});

        let size_sorted_log_tuples: Vec<_> = size_sorted_tuples.iter()
            .map(|x| {(x.0, (x.1 as f64).log2())}).collect();

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

        // now levels contains vectors, each of which is one level





        // based on log sizes, quantize into levels
        // given quantized levels, find valid merges within each level


        // how to find a valid merge

        // quantizing into levels
        // find the least segment within LEVEL_LOG_SIZE of the starting max size. it and everything above are the top level.
        // next, find the max of all other segments. this is the new max size. repeat. so LEVEL_LOG_SIZE basically is what determines how big your segments are.
        unimplemented!();
    }
}

impl Default for LogMergePolicy {
    fn default() -> LogMergePolicy {
        LogMergePolicy
    }
}
