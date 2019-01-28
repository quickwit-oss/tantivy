use census::{Inventory, TrackedObject};
use std::collections::HashSet;
use SegmentId;

#[derive(Default)]
pub struct MergeOperationInventory(Inventory<InnerMergeOperation>);

impl MergeOperationInventory {
    pub fn segment_in_merge(&self) -> HashSet<SegmentId> {
        let mut segment_in_merge = HashSet::default();
        for merge_op in self.0.list() {
            for &segment_id in &merge_op.segment_ids {
                segment_in_merge.insert(segment_id);
            }
        }
        segment_in_merge
    }
}

/// A `MergeOperation` has two role.
/// It carries all of the information required to describe a merge :
/// - `target_opstamp` is the opstamp up to which we want to consume the
/// delete queue and reflect their deletes.
/// - `segment_ids` is the list of segment to be merged.
///
/// The second role is to ensure keep track of the fact that these
/// segments are in merge and avoid starting a merge operation that
/// may conflict with this one.
///
/// This works by tracking merge operations. When considering computing
/// merge candidates, we simply list tracked merge operations and remove
/// their segments from possible merge candidates.
pub struct MergeOperation {
    inner: TrackedObject<InnerMergeOperation>,
}

struct InnerMergeOperation {
    target_opstamp: u64,
    segment_ids: Vec<SegmentId>,
}

impl MergeOperation {
    pub fn new(
        inventory: &MergeOperationInventory,
        target_opstamp: u64,
        segment_ids: Vec<SegmentId>,
    ) -> MergeOperation {
        let inner_merge_operation = InnerMergeOperation {
            target_opstamp,
            segment_ids,
        };
        MergeOperation {
            inner: inventory.0.track(inner_merge_operation),
        }
    }

    pub fn target_opstamp(&self) -> u64 {
        self.inner.target_opstamp
    }

    pub fn segment_ids(&self) -> &[SegmentId] {
        &self.inner.segment_ids[..]
    }
}
