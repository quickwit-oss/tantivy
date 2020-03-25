use crate::indexer::resource_manager::{Allocation, ResourceManager};
use crate::Opstamp;
use crate::SegmentId;
use census::{Inventory, TrackedObject};
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;

#[derive(Default, Clone)]
pub(crate) struct MergeOperationInventory {
    inventory: Inventory<InnerMergeOperation>,
    num_merge_watcher: ResourceManager,
}

impl Deref for MergeOperationInventory {
    type Target = Inventory<InnerMergeOperation>;

    fn deref(&self) -> &Self::Target {
        &self.inventory
    }
}

impl MergeOperationInventory {
    pub fn segment_in_merge(&self) -> HashSet<SegmentId> {
        let mut segment_in_merge = HashSet::default();
        for merge_op in self.list() {
            for &segment_id in &merge_op.segment_ids {
                segment_in_merge.insert(segment_id);
            }
        }
        segment_in_merge
    }

    pub fn wait_until_empty(&self) {
        let _ = self.num_merge_watcher.wait_until_in_range(0..1);
    }
}

/// A `MergeOperation` has two roles.
/// It carries all of the information required to describe a merge:
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

pub(crate) struct InnerMergeOperation {
    target_opstamp: Opstamp,
    segment_ids: Vec<SegmentId>,
    _allocation: Allocation,
}

impl fmt::Debug for InnerMergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MergeOp(target_opstamp={:?}, segment_ids={:?})",
            self.target_opstamp, self.segment_ids
        )
    }
}

impl MergeOperation {
    pub(crate) fn new(
        inventory: &MergeOperationInventory,
        target_opstamp: Opstamp,
        segment_ids: Vec<SegmentId>,
    ) -> MergeOperation {
        let allocation = inventory.num_merge_watcher.allocate(1);
        let inner_merge_operation = InnerMergeOperation {
            target_opstamp,
            segment_ids,
            _allocation: allocation,
        };
        MergeOperation {
            inner: inventory.track(inner_merge_operation),
        }
    }

    pub fn target_opstamp(&self) -> Opstamp {
        self.inner.target_opstamp
    }

    pub fn segment_ids(&self) -> &[SegmentId] {
        &self.inner.segment_ids[..]
    }
}
