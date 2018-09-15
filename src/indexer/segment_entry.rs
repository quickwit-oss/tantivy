use bit_set::BitSet;
use core::SegmentId;
use core::SegmentMeta;
use indexer::delete_queue::DeleteCursor;
use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SegmentState {
    Ready,
    InMerge,
}

impl SegmentState {
    pub fn letter_code(self) -> char {
        match self {
            SegmentState::InMerge => 'M',
            SegmentState::Ready => 'R',
        }
    }
}

/// A segment entry describes the state of
/// a given segment, at a given instant.
///
/// In addition to segment `meta`,
/// it contains a few transient states
/// - `state` expresses whether the segment is already in the
/// middle of a merge
/// - `delete_bitset` is a bitset describing
/// documents that were deleted during the commit
/// itself.
/// - `delete_cursor` is the position in the delete queue.
/// Deletes happening before the cursor are reflected either
/// in the .del file or in the `delete_bitset`.
#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    state: SegmentState,
    delete_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor,
}

impl SegmentEntry {
    /// Create a new `SegmentEntry`
    pub fn new(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor,
        delete_bitset: Option<BitSet>,
    ) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            state: SegmentState::Ready,
            delete_bitset,
            delete_cursor,
        }
    }

    /// Return a reference to the segment entry deleted bitset.
    ///
    /// `DocId` in this bitset are flagged as deleted.
    pub fn delete_bitset(&self) -> Option<&BitSet> {
        self.delete_bitset.as_ref()
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Return a reference to the segment_entry's delete cursor
    pub fn delete_cursor(&mut self) -> &mut DeleteCursor {
        &mut self.delete_cursor
    }

    /// Return the `SegmentEntry`.
    ///
    /// The state describes whether the segment is available for
    /// a merge or not.
    pub fn state(&self) -> SegmentState {
        self.state
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// Mark the `SegmentEntry` as in merge.
    ///
    /// Only segments that are not already
    /// in a merge are elligible for future merge.
    pub fn start_merge(&mut self) {
        self.state = SegmentState::InMerge;
    }

    /// Cancel a merge
    ///
    /// If a merge fails, it is important to switch
    /// the segment back to a idle state, so that it
    /// may be elligible for future merges.
    pub fn cancel_merge(&mut self) {
        self.state = SegmentState::Ready;
    }

    /// Returns true iff a segment should
    /// be considered for a merge.
    pub fn is_ready(&self) -> bool {
        self.state == SegmentState::Ready
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?}, {:?})", self.meta, self.state)
    }
}
