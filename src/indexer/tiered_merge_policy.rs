use std::collections::VecDeque;
use std::mem;

use itertools::Itertools;

use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::index::SegmentMeta;
use crate::{Result, TantivyError};

const MAX_EXPONENT: u8 = 27;

/// Builder for [`TieredMergePolicy`](TieredMergePolicy)
#[derive(Debug)]
pub struct TieredMergePolicyBuilder {
    min_exp: u8,
    max_exp: u8,
    step: u8,
    delete_ratio: f32,
    min_segments: Option<u32>,
}

impl Default for TieredMergePolicyBuilder {
    /// Create a new builder with default values
    ///
    /// - `min_exp`: `3`
    /// - `max_exp`: `24`
    /// - `step`: `3`
    /// - `delete_ratio`: `0.3`
    /// - `min_segments`: `2^step`
    fn default() -> Self {
        Self {
            // Initial target of 8 docs
            min_exp: 3,
            // Maximum target of 16M docs (hard limit of 32M)
            max_exp: 24,
            // Jump by 3 powers of 2 for each level (factor of 8)
            step: 3,
            // Require a segment with a max doc above 16M to have at least 33%
            // deletes before considering it for merging again.
            delete_ratio: 0.3,
            // Default to 2^step unless specified otherwise
            min_segments: None,
        }
    }
}

impl TieredMergePolicyBuilder {
    /// Set the starting exponent for the merge policy. This defines the first target size that the
    /// policy will attempt to fill.
    ///
    /// Default: `3`, so the policy will start by attempting to create segments with `8` (`2^3`)
    /// docs.
    pub fn with_min_exp(self, min_exp: u8) -> Self {
        Self { min_exp, ..self }
    }

    /// Set maximum exponent for the merge policy. This defines the final target size that the
    /// policy will attempt to fill.
    ///
    /// Default: `24`, so the policy will attempt to create segments with a final target of 16.7M
    /// (`2^32`) docs (hard limit of ~33M docs)
    pub fn with_max_exp(self, max_exp: u8) -> Self {
        Self { max_exp, ..self }
    }

    /// Set the size of the jump between tiers.
    ///
    /// `max_exp - min_exp` must be divisible by this value to ensure that levels are evenly
    /// distributed.
    ///
    /// Default: `3`, so each level will add `3` to the previous exponent.
    /// With the default `min_exp` of `3` and `max_exp` of `24`; a step of `3` will create tiers at
    /// `2^3`, `2^6`, `2^9`, ..., `2^24`, etc.
    pub fn with_step(self, step: u8) -> Self {
        Self { step, ..self }
    }

    /// Set the minimum number of segments required for a merge to be accepted.
    /// Decreasing this value will make the policy adhere more closely to the configured targets,
    /// but will result in more merge operations. A merge must always have enough documents to hit
    /// the next tier regardless of this setting.
    ///
    /// Default: `2^step` to match the scale factor of the levels
    pub fn with_min_segments(self, min_segments: u32) -> Self {
        Self {
            min_segments: Some(min_segments),
            ..self
        }
    }

    /// Set the ratio for which large segments will be reconsidered for merging. This only applies
    /// to segments above the maximum target and defines the ratio of deletes they must have before
    /// the policy will merge them again. Regardless of what this setting is configured to, the
    /// segment must have enough deletes to bring the number of alive docs below the target before
    /// it will be considered.
    ///
    /// Default: `0.3`, so a segment must have at least 30% deletions before it will be
    /// reconsidered.
    pub fn with_delete_ratio(self, delete_ratio: f32) -> Self {
        Self {
            delete_ratio,
            ..self
        }
    }

    /// Validate the configuration and construct the policy
    pub fn build(self) -> Result<TieredMergePolicy> {
        let Self {
            min_exp,
            max_exp,
            step,
            delete_ratio,
            min_segments,
        } = self;

        if !(0.0..=1.0).contains(&delete_ratio) {
            return Result::Err(TantivyError::InvalidArgument(String::from(
                "delete_ratio must be between 0.0 and 1.0",
            )));
        }

        if min_exp >= max_exp {
            return Result::Err(TantivyError::InvalidArgument(String::from(
                "min_exp must be less than max_exp",
            )));
        }

        if max_exp > MAX_EXPONENT {
            return Result::Err(TantivyError::InvalidArgument(format!(
                "Max exponent {max_exp} is greater than the allowed {MAX_EXPONENT}"
            )));
        }

        if (max_exp - min_exp) % step != 0 {
            return Result::Err(TantivyError::InvalidArgument(String::from(
                "max_exp - min_exp must be divisible by step",
            )));
        }

        Ok(TieredMergePolicy {
            min_exp,
            max_exp,
            step,
            delete_ratio,
            target: 1 << max_exp,
            min_segments: min_segments.unwrap_or_else(|| 1 << step) as usize,
        })
    }
}

/// A tiered merge policy that groups segments into exponential size
/// levels and merges them bottom-up.
///
/// Segments are bucketed by document count into levels defined by powers of two,
/// starting at `2^min_exp` and increasing by `2^step` up to `2^max_exp`. At each
/// level, segments smaller than that level's threshold are batched together. Once
/// a batch accumulates enough documents to meet the level's target, it becomes a
/// merge candidate.
///
/// To avoid redundant small merges, candidates from smaller levels are held in a
/// pool and can be consumed by larger levels when enough total documents exist to
/// form a bigger segment. Leftover segments that don't fill a batch are carried
/// upward to the next level.
///
/// Segments that already meet or exceed the maximum target size (`2^max_exp`) are
/// never selected for merging unless they have a sufficient number of deletes.
///
/// The policy will never output a segment larger than `(2 * 2^max_exp) - 2`
///
/// # Parameters
///
/// - `min_exp`: The starting exponent. Levels begin at `2^min_exp`.
/// - `max_exp`: The maximum exponent. Segments with `>= 2^max_exp` docs are considered fully
///   merged. Must be `<= 27` to avoid `u32` overflow.
/// - `step`: The exponent increment between levels. `max_exp - min_exp` must be divisible by
///   `step`. Also controls the minimum number of segments required per merge candidate (`2^step`)
///   if not manually specified.
/// - `delete_ratio`: The ratio of deleted to active documents. This is only considered for segments
///   above the target size and is used to make them eligible for re-merging. Must be between `0.0`
///   and `1.0`.
///
/// # Example
///
/// ```ignore
/// // Levels at 2^3, 2^6, 2^9, ..., 2^24 with min 8 segments per merge.
/// let policy = TieredMergePolicyBuilder::default()
///                 .with_min_exp(3)
///                 .with_max_exp(24)
///                 .with_step(3)
///                 .build();
/// ```
#[derive(Debug)]
pub struct TieredMergePolicy {
    min_exp: u8,
    max_exp: u8,
    step: u8,
    delete_ratio: f32,
    target: u32,
    min_segments: usize,
}

impl TieredMergePolicy {
    /// Check if the number of alive documents in the segment is below the target size and that the
    /// deletion ratio is at least the configured target
    fn hit_target_delete_threshold(&self, segment: &SegmentMeta) -> bool {
        let max_doc = segment.max_doc();
        let deleted = segment.num_deleted_docs();
        if deleted == 0 {
            return false;
        }
        (max_doc - deleted < self.target) && (deleted as f32 > max_doc as f32 * self.delete_ratio)
    }
}

impl MergePolicy for TieredMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        let mut segments = segments
            .iter()
            .filter_map(|segment| {
                if segment.max_doc() < self.target || self.hit_target_delete_threshold(segment) {
                    Some((segment.num_docs() as usize, segment.id()))
                } else {
                    None
                }
            })
            .sorted_unstable_by_key(|(docs, _)| *docs)
            .peekable();

        // The smallest segment size, if iterator is empty then exit
        let Some(smallest) = segments.peek().map(|(docs, _)| *docs) else {
            return Vec::new();
        };

        // Pool for potential merge candidates
        let mut pool = VecDeque::new();
        let mut pool_docs = 0;

        // The current batch of segments to merge
        let mut current = Vec::new();
        let mut current_docs = 0;

        // Leftover segments that were not merged in their native level
        let mut carry = Vec::new();
        let mut carry_docs = 0;

        // Start at the smallest exponent, then skip any targets smaller than the first candidate
        let mut current_exponent = self.min_exp;
        while 1 << current_exponent < smallest {
            current_exponent += self.step;
        }

        while current_exponent <= self.max_exp {
            let target = 1 << current_exponent;

            // Number of docs in the pool before starting this iteration
            let mut pre_pool_docs = pool_docs;

            // Break if there aren't enough docs left to create any more merges
            if segments.peek().is_none() && carry_docs + pool_docs < target {
                break;
            }

            // Get all segments that are less than the target size for this level and make a new
            // candidate whenever the batch hits the target size
            for (docs, id) in segments.peeking_take_while(|(docs, _)| *docs < target) {
                current_docs += docs;
                current.push(id);

                // Add to the pool if we hit the target conditions
                if (current_docs >= target && current.len() >= self.min_segments)
                    || current_docs as u32 >= self.target
                {
                    pool_docs += current_docs;
                    pool.push_back((mem::take(&mut current_docs), mem::take(&mut current)));
                }
            }

            // If there are enough leftover docs in the current batch, carry, and merges from
            // smaller levels then use them to make larger segments to prevent redundant
            // smaller merges
            if current_docs + carry_docs + pre_pool_docs >= target {
                // The number of docs in carry will always be less than the previous target
                // otherwise they would have been merged already
                current.append(&mut carry);
                current_docs += carry_docs;
                carry_docs = 0;

                loop {
                    // Add to the pool if we hit the target conditions
                    if (current_docs >= target && current.len() >= self.min_segments)
                        || current_docs as u32 >= self.target
                    {
                        pool_docs += current_docs;
                        pool.push_back((mem::take(&mut current_docs), mem::take(&mut current)));
                    }

                    // Pop smaller merge candidates off the pool if there are enough docs to create
                    // an even bigger segment
                    //
                    // New merge candidates are always placed at the back of the pool, since we loop
                    // through segments in ascending order this means that the pool will be in
                    // roughly ascending order. Individual merge candidates may not be in order, but
                    // all candidates for a given tier will be placed together. By checking the
                    // front of the pool we ensure we get merge candidates from the smallest tiers
                    // first to maximise tier skipping for small segments. We only take candidates
                    // from tiers smaller than the current iteration.
                    if current_docs + pre_pool_docs >= target
                        && pool.front().is_some_and(|(docs, _)| *docs < target)
                    {
                        let (docs, candidate) = pool.pop_front().unwrap();
                        current_docs += docs;
                        pre_pool_docs -= docs;
                        pool_docs -= docs;
                        current.extend(candidate);
                    } else {
                        break;
                    }
                }
            }

            carry_docs += mem::take(&mut current_docs);
            carry.append(&mut current);
            current_exponent += self.step;
        }

        // All candidates should have at least 2 segments and never exceed 2x the target size
        debug_assert!(pool
            .iter()
            .all(|(docs, _)| (*docs as u32) < self.target * 2));
        debug_assert!(pool.iter().all(|(_, segments)| segments.len() >= 2));

        // Pool will be in roughly ascending order, the tiers will be ascending but segments within
        // a tier may be out of order
        pool.into_iter()
            .map(|(_, candidate)| MergeCandidate(candidate))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use once_cell::sync::Lazy;

    use super::*;
    use crate::index::{SegmentId, SegmentMetaInventory};

    static INVENTORY: Lazy<SegmentMetaInventory> = Lazy::new(SegmentMetaInventory::default);

    fn create_random_segment_meta(num_docs: u32) -> SegmentMeta {
        INVENTORY.new_segment_meta(SegmentId::generate_random(), num_docs)
    }

    /// Create a test policy with tiers every power of 2 between 8 and 4096
    fn test_merge_policy() -> TieredMergePolicy {
        TieredMergePolicyBuilder::default()
            .with_min_exp(3)
            .with_max_exp(12)
            .with_step(1)
            .build()
            .unwrap()
    }

    #[test]
    fn test_tiered_merge_policy_empty() {
        let test_input = vec![];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_tiered_merge_policy_filter_target() {
        // All segments are at or above the target and should be ignored
        let test_input = vec![
            create_random_segment_meta(4096),
            create_random_segment_meta(4096),
            create_random_segment_meta(4096),
            create_random_segment_meta(4096),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_tiered_merge_policy_no_merge_small_levels() {
        // 1 segment for each target below the max shouldn't produce any merges since there aren't
        // enough docs to increase the tier of any segment
        let test_input = vec![
            create_random_segment_meta(8),
            create_random_segment_meta(16),
            create_random_segment_meta(32),
            create_random_segment_meta(64),
            create_random_segment_meta(128),
            create_random_segment_meta(256),
            create_random_segment_meta(512),
            create_random_segment_meta(1024),
            create_random_segment_meta(2048),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_tiered_merge_policy_multiple_levels() {
        // This should create 2 merges, one for the 2 small segments and another for the large
        // segments. No carrying/pooling should occur since there aren't enough docs in the small
        // segments
        let test_input = vec![
            create_random_segment_meta(64),
            create_random_segment_meta(64),
            create_random_segment_meta(512),
            create_random_segment_meta(512),
        ];

        let small_ids: HashSet<_> = vec![test_input[0].id(), test_input[1].id()]
            .into_iter()
            .collect();
        let large_ids: HashSet<_> = vec![test_input[2].id(), test_input[3].id()]
            .into_iter()
            .collect();

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 2);
        assert_eq!(result_list[0].0.len(), 2);
        assert_eq!(result_list[1].0.len(), 2);

        // The first merge should contain both small segments
        for segment in &result_list[0].0 {
            assert!(small_ids.contains(segment));
        }

        // The second merge should contain both large segments
        for segment in &result_list[1].0 {
            assert!(large_ids.contains(segment));
        }
    }

    #[test]
    fn test_tiered_merge_policy_carry() {
        // No indivdual tier has enough documents to reach the next target, but carrying small
        // segments has enough to reach the next tier of 512 so all segments should be merged in
        // this case.
        let test_input = vec![
            create_random_segment_meta(20),
            create_random_segment_meta(100),
            create_random_segment_meta(400),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 3);
    }

    #[test]
    fn test_tiered_merge_policy_pool() {
        // The 2 small segments will meet the merge criteria to hit the target of 128, but this
        // candidate should then be consumed since there are enough total documents to hit the next
        // tier of 512 docs.
        let test_input = vec![
            create_random_segment_meta(64),
            create_random_segment_meta(64),
            create_random_segment_meta(400),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 3);
    }

    #[test]
    fn test_tiered_merge_policy_pool_aggregate_target() {
        // 8096 total docs as 1024 tiny segments. This should get merged into 2 segments of 4096
        // docs.
        let test_input = (0..1024)
            .map(|_| create_random_segment_meta(8))
            .collect_vec();

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);

        assert_eq!(result_list.len(), 2);
        assert_eq!(result_list[0].0.len(), 512);
        assert_eq!(result_list[1].0.len(), 512);
    }

    #[test]
    fn test_tiered_merge_policy_not_enough_delete_ratio() {
        // 2 segments with 3076 docs each, but only a delete ratio of ~0.25
        let test_input = vec![
            create_random_segment_meta(4100).with_delete_meta(1024, 1),
            create_random_segment_meta(4100).with_delete_meta(1024, 1),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_tiered_merge_policy_delete_ratio_too_many_docs() {
        // 2 large segments with a high delete ratio, but not enough deletes to be below the target
        let test_input = vec![
            create_random_segment_meta(7168).with_delete_meta(3000, 1),
            create_random_segment_meta(7168).with_delete_meta(3000, 1),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert!(result_list.is_empty());
    }

    #[test]
    fn test_tiered_merge_policy_with_deletes() {
        // 2 large segments with enough deletes to go below the target number of docs, and a high
        // enough delete ratio to be considered.
        let test_input = vec![
            create_random_segment_meta(6144).with_delete_meta(2500, 1),
            create_random_segment_meta(6144).with_delete_meta(2500, 1),
        ];

        let result_list = test_merge_policy().compute_merge_candidates(&test_input);
        assert_eq!(result_list.len(), 1);
        assert_eq!(result_list[0].0.len(), 2);
    }
}
