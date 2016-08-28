use postings::block_store::BlockStore;
use DocId;
use std::io;
use postings::PostingsSerializer;


const EMPTY_ARRAY: [u32; 0] = [0u32; 0];
const POSITION_END: u32 = 4294967295; 

pub trait Recorder {
    fn current_doc(&self,) -> u32;
    fn new(block_store: &mut BlockStore) -> Self;
    fn new_doc(&mut self, block_store: &mut BlockStore, doc: DocId);
    fn record_position(&mut self, block_store: &mut BlockStore, position: u32);
    fn close_doc(&mut self, block_store: &mut BlockStore);
    fn doc_freq(&self,) -> u32;
    
    fn serialize(&self, serializer: &mut PostingsSerializer, block_store: &BlockStore) -> io::Result<()>;
}

pub struct NothingRecorder {
    list_id: u32,
    current_doc: DocId,
    doc_freq: u32,
}

impl Recorder for NothingRecorder {
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }
    
    fn new(block_store: &mut BlockStore) -> Self {
        NothingRecorder {
            list_id: block_store.new_list(),
            current_doc: u32::max_value(),
            doc_freq: 0u32,
        }
    }
    
    fn new_doc(&mut self, block_store: &mut BlockStore, doc: DocId) {
        self.current_doc = doc;
        block_store.push(self.list_id, doc);
        self.doc_freq += 1;
    }
    
    fn record_position(&mut self, _block_store: &mut BlockStore, _position: u32) {
    }
    
    fn close_doc(&mut self, _block_store: &mut BlockStore) {
    }
    
    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    fn serialize(&self, serializer: &mut PostingsSerializer, block_store: &BlockStore) -> io::Result<()> {
        let doc_id_iter = block_store.iter_list(self.list_id);
        for doc in doc_id_iter {
            try!(serializer.write_doc(doc, 0u32, &EMPTY_ARRAY));
        }
        Ok(())
    }
}



pub struct TermFrequencyRecorder {
    list_id: u32,
    current_doc: DocId,
    current_tf: u32,
    doc_freq: u32,
}

impl Recorder for TermFrequencyRecorder {
    
    fn new(block_store: &mut BlockStore) -> Self {
        TermFrequencyRecorder {
            list_id: block_store.new_list(),
            current_doc: u32::max_value(),
            current_tf: 0u32,
            doc_freq: 0u32,
        }
    }
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }
    
    fn new_doc(&mut self, block_store: &mut BlockStore, doc: DocId) {
        self.doc_freq += 1u32;
        self.current_doc = doc;
        block_store.push(self.list_id, doc);
    }
    
    fn record_position(&mut self, _block_store: &mut BlockStore, _position: u32) {
        self.current_tf += 1;
    }
    
    fn close_doc(&mut self, block_store: &mut BlockStore) {
        assert!(self.current_tf > 0);
        block_store.push(self.list_id, self.current_tf);
        self.current_tf = 0;
    }
    
    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    fn serialize(&self, serializer: &mut PostingsSerializer, block_store: &BlockStore) -> io::Result<()> {
        let mut doc_iter = block_store.iter_list(self.list_id);
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



pub struct TFAndPositionRecorder {
    list_id: u32,
    current_doc: DocId,
    doc_freq: u32,
}


impl Recorder for TFAndPositionRecorder {
    
    fn new(block_store: &mut BlockStore) -> Self {
        TFAndPositionRecorder {
            list_id: block_store.new_list(),
            current_doc: u32::max_value(),
            doc_freq: 0u32,
        }
    }
    
    fn current_doc(&self,) -> DocId {
        self.current_doc
    }
    
    fn new_doc(&mut self, block_store: &mut BlockStore, doc: DocId) {
        self.doc_freq += 1;
        self.current_doc = doc;
        block_store.push(self.list_id, doc);
    }
    
    fn record_position(&mut self, block_store: &mut BlockStore, position: u32) {
        block_store.push(self.list_id, position);
    }
    
    fn close_doc(&mut self, block_store: &mut BlockStore) {
        block_store.push(self.list_id, POSITION_END);
    }
    
    fn doc_freq(&self,) -> u32 {
        self.doc_freq
    }
    
    
    fn serialize(&self, serializer: &mut PostingsSerializer, block_store: &BlockStore) -> io::Result<()> {
        let mut positions = Vec::with_capacity(100);
        let mut doc_iter = block_store.iter_list(self.list_id);
        loop {
            if let Some(doc) = doc_iter.next() {
                let mut prev_position = 0;
                positions.clear();
                loop {
                    match doc_iter.next() {
                        Some(position) => {
                            if position == POSITION_END {
                                break;
                            }
                            else {
                                positions.push(position - prev_position);
                                prev_position = position;
                            }
                        }
                        None => {
                            panic!("This should never happen. Pleasee report the bug.");
                        }
                    }
                }
                try!(serializer.write_doc(doc, positions.len() as u32, &positions));
            }
            else {
                break;
            }
        }
        Ok(())
    }
}


