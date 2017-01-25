use core::SegmentId;
use core::SegmentMeta;
use std::marker;
use std::fmt::Debug;


/// Set of segment suggested for a merge. 
#[derive(Debug, Clone)]
pub struct MergeCandidate(pub Vec<SegmentId>);


/// The Merge policy defines which segments should be merged. 
/// 
/// Every time a the list of segments changes, the segment updater
/// asks the merge policy if some segments should be merged.
pub trait MergePolicy: marker::Send + marker::Sync + Debug {
    /// Given the list of segment metas, returns the list of merge candidates. 
    ///
    /// This call happens on the segment updater thread, and will block 
    /// other segment updates, so all implementations should happen rapidly. 
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>;
    /// Returns a boxed clone of the MergePolicy.
    fn box_clone(&self) -> Box<MergePolicy>;
}

/// Never merge segments. 
#[derive(Debug)]
pub struct NoMergePolicy;

impl Default for NoMergePolicy {
    fn default() -> NoMergePolicy {
        NoMergePolicy
    }
}

impl MergePolicy for NoMergePolicy {
    fn compute_merge_candidates(&self, _segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        Vec::new()
    }
    
    fn box_clone(&self) -> Box<MergePolicy> {
        box NoMergePolicy
    }
}

