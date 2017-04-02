use core::SegmentMeta;
use bit_set::BitSet;
use indexer::delete_queue::DeleteCursor;
use core::SegmentId;
use std::fmt;


#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SegmentState {
    Ready,
    InMerge,        
}

impl SegmentState {
    pub fn letter_code(&self,) -> char {
        match *self {
            SegmentState::InMerge => 'M',
            SegmentState::Ready => 'R',
        }
    }
}

#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
    state: SegmentState,
    delete_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor,

}

impl SegmentEntry {

    pub fn new(segment_meta: SegmentMeta, 
               delete_cursor: DeleteCursor,
               delete_bitset: Option<BitSet>) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            state: SegmentState::Ready,
            delete_bitset: delete_bitset,
            delete_cursor: delete_cursor,
        }
    }

    pub fn delete_bitset(&self,) -> Option<&BitSet> {
        self.delete_bitset.as_ref()
    }

    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    pub fn delete_cursor(&mut self) -> &mut DeleteCursor {
        &mut self.delete_cursor
    }

    pub fn state(&self) -> SegmentState {
        self.state
    }

    pub fn set_state(&mut self, state: SegmentState) {
        self.state = state;
    }

    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }
    
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn start_merge(&mut self,) {
        self.state = SegmentState::InMerge;
    }

    pub fn cancel_merge(&mut self,) {
        self.state = SegmentState::Ready;
    }

    pub fn is_ready(&self,) -> bool {
        self.state == SegmentState::Ready
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?}, {:?})", self.meta, self.state)
    }
}
