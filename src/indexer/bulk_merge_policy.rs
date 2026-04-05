use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::index::SegmentMeta;

/// A merge policy optimized for bulk indexing workloads.
///
/// Unlike [`LogMergePolicy`](super::LogMergePolicy), this policy is designed
/// to minimize merge I/O during heavy indexing by:
/// - Requiring more segments before triggering a merge (default: 16 vs 8)
/// - Only merging segments of similar size (logarithmic grouping)
/// - Setting a higher minimum segment size to avoid merging tiny segments
///   that will soon be merged again anyway
///
/// After bulk indexing is complete, call
/// [`IndexWriter::merge`](crate::IndexWriter::merge) with all segment IDs
/// to consolidate into a single segment.
///
/// # Example
/// ```rust,no_run
/// use tantivy::indexer::BulkIndexingMergePolicy;
///
/// let mut policy = BulkIndexingMergePolicy::default();
/// policy.set_min_num_segments(20);  // even more conservative
/// ```
#[derive(Debug, Clone)]
pub struct BulkIndexingMergePolicy {
    /// Minimum segments at a level before merge is triggered.
    min_num_segments: usize,
    /// Maximum docs in a segment for it to be eligible for merge.
    max_docs_before_merge: usize,
    /// Minimum segment size for level grouping.
    min_layer_size: u32,
    /// Log base for level grouping.
    level_log_size: f64,
}

impl Default for BulkIndexingMergePolicy {
    fn default() -> Self {
        BulkIndexingMergePolicy {
            // High threshold: wait for many segments before merging
            min_num_segments: 16,
            // Don't merge very large segments during bulk indexing
            max_docs_before_merge: 5_000_000,
            // Larger minimum layer: treat all segments under 50K as same level
            min_layer_size: 50_000,
            // Wider level bands to group more segments together
            level_log_size: 1.0,
        }
    }
}

impl BulkIndexingMergePolicy {
    /// Set the minimum number of segments required at a level to trigger a merge.
    pub fn set_min_num_segments(&mut self, min_num_segments: usize) {
        self.min_num_segments = min_num_segments;
    }

    /// Set the maximum number of documents in a segment for it to be eligible for merge.
    pub fn set_max_docs_before_merge(&mut self, max_docs: usize) {
        self.max_docs_before_merge = max_docs;
    }

    /// Set the minimum segment size for level grouping.
    pub fn set_min_layer_size(&mut self, min_layer_size: u32) {
        self.min_layer_size = min_layer_size;
    }

    fn clip_min_size(&self, size: u32) -> u32 {
        std::cmp::max(self.min_layer_size, size)
    }
}

impl MergePolicy for BulkIndexingMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        use itertools::Itertools;

        let eligible_segments: Vec<&SegmentMeta> = segments
            .iter()
            .filter(|seg| (seg.num_docs() as usize) <= self.max_docs_before_merge)
            .sorted_by_key(|seg| std::cmp::Reverse(seg.max_doc()))
            .collect();

        if eligible_segments.is_empty() {
            return vec![];
        }

        let mut current_max_log_size = f64::MAX;
        let mut levels: Vec<Vec<&SegmentMeta>> = vec![];

        for (_, group) in &eligible_segments.into_iter().chunk_by(|segment| {
            let segment_log_size = f64::from(self.clip_min_size(segment.num_docs())).log2();
            if segment_log_size < (current_max_log_size - self.level_log_size) {
                current_max_log_size = segment_log_size;
            }
            current_max_log_size
        }) {
            levels.push(group.collect());
        }

        levels
            .iter()
            .filter(|level| level.len() >= self.min_num_segments)
            .map(|segments| MergeCandidate(segments.iter().map(|seg| seg.id()).collect()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{SegmentId, SegmentMetaInventory};
    use once_cell::sync::Lazy;

    static INVENTORY: Lazy<SegmentMetaInventory> = Lazy::new(SegmentMetaInventory::default);

    fn create_segment_meta(num_docs: u32) -> SegmentMeta {
        INVENTORY.new_segment_meta(SegmentId::generate_random(), num_docs)
    }

    #[test]
    fn test_bulk_policy_no_merge_below_threshold() {
        let policy = BulkIndexingMergePolicy::default();
        // 15 segments at the same level — still below threshold of 16
        let segments: Vec<SegmentMeta> = (0..15).map(|_| create_segment_meta(10_000)).collect();
        let candidates = policy.compute_merge_candidates(&segments);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_bulk_policy_merge_at_threshold() {
        let policy = BulkIndexingMergePolicy::default();
        // 16 segments at the same level — reaches threshold
        let segments: Vec<SegmentMeta> = (0..16).map(|_| create_segment_meta(10_000)).collect();
        let candidates = policy.compute_merge_candidates(&segments);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0.len(), 16);
    }

    #[test]
    fn test_bulk_policy_skips_large_segments() {
        let policy = BulkIndexingMergePolicy::default();
        // 20 segments but all above max_docs_before_merge
        let segments: Vec<SegmentMeta> =
            (0..20).map(|_| create_segment_meta(6_000_000)).collect();
        let candidates = policy.compute_merge_candidates(&segments);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_bulk_policy_separate_levels() {
        let policy = BulkIndexingMergePolicy::default();
        // 16 small + 16 medium — should produce 2 merge candidates
        let mut segments: Vec<SegmentMeta> =
            (0..16).map(|_| create_segment_meta(1_000)).collect();
        segments.extend((0..16).map(|_| create_segment_meta(500_000)));
        let candidates = policy.compute_merge_candidates(&segments);
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn test_bulk_policy_empty() {
        let policy = BulkIndexingMergePolicy::default();
        let candidates = policy.compute_merge_candidates(&[]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_bulk_policy_custom_thresholds() {
        let mut policy = BulkIndexingMergePolicy::default();
        policy.set_min_num_segments(4);
        let segments: Vec<SegmentMeta> = (0..4).map(|_| create_segment_meta(10_000)).collect();
        let candidates = policy.compute_merge_candidates(&segments);
        assert_eq!(candidates.len(), 1);
    }
}
