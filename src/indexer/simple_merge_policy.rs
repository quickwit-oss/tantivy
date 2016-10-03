use super::merge_policy::{MergePolicy, MergeCandidate};
use core::SegmentMeta;

pub struct SimpleMergePolicy;

const PACK_LEN: usize = 8;

impl MergePolicy for SimpleMergePolicy {
    
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let num_packs = segments.len() / PACK_LEN; 
        (0..num_packs)
            .map(|i| {
                let segment_ids = segments[i..i*PACK_LEN]
                    .iter()
                    .map(|segment_meta| segment_meta.segment_id)
                    .collect();
                MergeCandidate(segment_ids)
            })
            .collect()
    }

}

impl Default for SimpleMergePolicy {
    fn default() -> SimpleMergePolicy {
        SimpleMergePolicy
    }
}