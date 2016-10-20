use core::SegmentId;
use core::SegmentMeta;
use std::marker;


#[derive(Debug, Clone)]
pub struct MergeCandidate(pub Vec<SegmentId>);

pub trait MergePolicy: marker::Send {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>;
}

pub struct NoMergePolicy;

impl MergePolicy for NoMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        Vec::new()
    }
}