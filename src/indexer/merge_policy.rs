use core::SegmentId;
use indexer::SegmentRegister;

pub struct MergeCandidate {
    segments: Vec<SegmentId>,
}

pub trait MergePolicy {
    fn merge_candidates(segments: &SegmentRegister) -> Vec<MergeCandidate>;
}