use crate::core::SegmentId;
use crate::core::SegmentMeta;
use std::fmt::Debug;
use std::marker;

/// Set of segment suggested for a merge.
#[derive(Debug, Clone)]
pub struct MergeCandidate(pub Vec<SegmentId>);

/// The `MergePolicy` defines which segments should be merged.
///
/// Every time a the list of segments changes, the segment updater
/// asks the merge policy if some segments should be merged.
pub trait MergePolicy: marker::Send + marker::Sync + Debug {
    /// Given the list of segment metas, returns the list of merge candidates.
    ///
    /// This call happens on the segment updater thread, and will block
    /// other segment updates, so all implementations should happen rapidly.
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate>;
}

/// Never merge segments.
#[derive(Debug, Clone)]
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
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::core::SegmentId;
    use crate::core::SegmentMeta;

    /// `MergePolicy` useful for test purposes.
    ///
    /// Everytime there is more than one segment,
    /// it will suggest to merge them.
    #[derive(Debug, Clone)]
    pub struct MergeWheneverPossible;

    impl MergePolicy for MergeWheneverPossible {
        fn compute_merge_candidates(&self, segment_metas: &[SegmentMeta]) -> Vec<MergeCandidate> {
            let segment_ids = segment_metas
                .iter()
                .map(|segment_meta| segment_meta.id())
                .collect::<Vec<SegmentId>>();
            if segment_ids.len() > 1 {
                vec![MergeCandidate(segment_ids)]
            } else {
                vec![]
            }
        }
    }
}
