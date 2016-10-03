use core::SegmentId;
use core::SegmentMeta;


#[derive(Debug)]
pub struct MergeCandidate(pub Vec<SegmentId>);

pub trait MergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>;
}
