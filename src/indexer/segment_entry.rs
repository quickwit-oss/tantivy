use indexer::doc_opstamp_mapping::DocToOpstampMapping;
use core::SegmentMeta;
use indexer::delete_queue::DeleteCursor;
use core::SegmentId;
use std::fmt;
use std::mem;

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
    doc_to_opstamp: DocToOpstampMapping,
    delete_cursor: DeleteCursor,

}

impl SegmentEntry {

    pub fn new(segment_meta: SegmentMeta, 
               delete_cursor: DeleteCursor) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            state: SegmentState::Ready,
            doc_to_opstamp: DocToOpstampMapping::None,
            delete_cursor: delete_cursor,
        }
    }

    pub fn reset_doc_to_stamp(&mut self,) -> DocToOpstampMapping {
        mem::replace(&mut self.doc_to_opstamp, DocToOpstampMapping::None)
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

    pub fn set_doc_to_opstamp(&mut self, doc_to_opstamp: DocToOpstampMapping) {
        self.doc_to_opstamp = doc_to_opstamp;
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

    pub fn is_ready(&self,) -> bool {
        self.state == SegmentState::Ready
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?}, {:?})", self.meta, self.state)
    }
}
