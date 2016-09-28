use core::SegmentId;
use indexer::segment_register::SegmentRegister;

pub struct MergeCandidate {
    segments: Vec<SegmentId>,
}

pub trait MergePolicy {
    fn merge_candidates(segments: &SegmentRegister) -> Vec<MergeCandidate>;
}