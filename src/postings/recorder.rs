use DocId;
use std::io;
use postings::PostingsSerializer;
use datastruct::stacker::{ExpUnrolledLinkedList, Heap};

const EMPTY_ARRAY: [u32; 0] = [0u32; 0];
const POSITION_END: u32 = 4294967295; 

pub trait Recorder: From<u32> {
    fn current_doc(&self,) -> u32;
    fn new_doc(&mut self, doc: DocId, heap: &Heap);
    fn record_position(&mut self, position: u32, heap: &Heap);
    fn close_doc(&mut self, heap: &Heap);
    fn doc_freq(&self,) -> u32;
    fn serialize(&self, self_addr: u32, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()>;
}

#[repr(C, packed)]
pub struct NothingRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    doc_freq: u32,
}

impl From<u32> for NothingRecorder {
    fn from(addr: u32) -> NothingRecorder {
        NothingRecorder {
            stack: ExpUnrolledLinkedList::from(addr),
            current_doc: u32::max_value(),
            doc_freq: 0u32,
        }
    }
}

impl Recorder for NothingRecorder {
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }
    
    
    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
        self.doc_freq += 1;
    }

    fn record_position(&mut self, _position: u32, _heap: &Heap) {}

    fn close_doc(&mut self, _heap: &Heap) {}

    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    fn serialize(&self, self_addr: u32, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()> {
        for doc in self.stack.iter(self_addr, heap) {
            try!(serializer.write_doc(doc, 0u32, &EMPTY_ARRAY));
        }
        Ok(())
    }
}


#[repr(C, packed)]
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
    doc_freq: u32,
}

impl From<u32> for TermFrequencyRecorder {
    fn from(addr: u32) -> TermFrequencyRecorder {
        TermFrequencyRecorder {
            stack: ExpUnrolledLinkedList::from(addr),
            current_doc: u32::max_value(),
            current_tf: 0u32,
            doc_freq: 0u32
        }    
    }
}

impl Recorder for TermFrequencyRecorder {
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.doc_freq += 1u32;
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }
    
    fn record_position(&mut self, _position: u32, _heap: &Heap) {
        self.current_tf += 1;
    }
    
    fn close_doc(&mut self, heap: &Heap) {
        debug_assert!(self.current_tf > 0);
        self.stack.push(self.current_tf, heap);
        self.current_tf = 0;
    }
    
    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    fn serialize(&self, self_addr:u32, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()> {
        let mut doc_iter = self.stack.iter(self_addr, heap);
        loop {
            if let Some(doc) = doc_iter.next() {
                if let Some(term_freq) = doc_iter.next() {
                    try!(serializer.write_doc(doc, term_freq, &EMPTY_ARRAY));
                    continue;
                }
            }
            break;
        }
        Ok(())
    }
}


#[repr(C, packed)]
pub struct TFAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    doc_freq: u32,
}

impl From<u32> for TFAndPositionRecorder {
    fn from(addr: u32) -> TFAndPositionRecorder {
        TFAndPositionRecorder {
            stack: ExpUnrolledLinkedList::from(addr),
            current_doc: u32::max_value(),
            doc_freq: 0u32,
        }
    }
    
}

impl Recorder for TFAndPositionRecorder {
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }
    
    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.doc_freq += 1;
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, position: u32, heap: &Heap) {
        self.stack.push(position, heap);
    }
    
    fn close_doc(&mut self, heap: &Heap) {
        self.stack.push(POSITION_END, heap);
    }
    
    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    fn serialize(&self, self_addr: u32, serializer: &mut PostingsSerializer, heap: &Heap) -> io::Result<()> {
        let mut doc_positions = Vec::with_capacity(100);
        let mut positions_iter = self.stack.iter(self_addr, heap);
        loop {
            if let Some(doc) = positions_iter.next() {
                let mut prev_position = 0;
                doc_positions.clear();
                loop {
                    match positions_iter.next() {
                        Some(position) => {
                            if position == POSITION_END {
                                break;
                            }
                            else {
                                doc_positions.push(position - prev_position);
                                prev_position = position;
                            }
                        }
                        None => {
                            panic!("This should never happen. Pleasee report the bug.");
                        }
                    }
                }
                try!(serializer.write_doc(doc, doc_positions.len() as u32, &doc_positions));
            }
            else {
                break;
            }
        }
        Ok(())
    }
}


