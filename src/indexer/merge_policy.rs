use core::SegmentId;
use core::SegmentMeta;
use std::marker;


#[derive(Debug, Clone)]
pub struct MergeCandidate(pub Vec<SegmentId>);

pub trait MergePolicy: marker::Send {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>;
}
