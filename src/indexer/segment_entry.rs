use indexer::doc_opstamp_mapping::DocToOpstampMapping;
use core::SegmentMeta;
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
    doc_to_opstamp: DocToOpstampMapping,
}

impl SegmentEntry {

    pub fn new(segment_meta: SegmentMeta) -> SegmentEntry {
        SegmentEntry {
            meta: segment_meta,
            state: SegmentState::Ready,
            doc_to_opstamp: DocToOpstampMapping::None,
        }
    }

    pub fn doc_to_opstamp(&self) -> &DocToOpstampMapping {
        &self.doc_to_opstamp
    }

    pub fn state(&self) -> SegmentState {
        self.state
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
