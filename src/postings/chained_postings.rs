use DocId;
use postings::{Postings, SkipResult};
use postings::OffsetPostings;

pub struct ChainedPostings<'a> {
    chained_postings: Vec<OffsetPostings<'a>>,
    posting_id: usize,
    doc_freq: usize,
}

impl<'a> ChainedPostings<'a> {
    
    pub fn new(chained_postings: Vec<OffsetPostings<'a>>) -> ChainedPostings {
        let mut doc_freq: usize = 0;
        for segment_postings in chained_postings.iter() {
            doc_freq += segment_postings.doc_freq();
        }
        ChainedPostings {
            chained_postings: chained_postings,
            posting_id: 0,
            doc_freq: doc_freq,
        }
    }
    

    pub fn freq(&self,) -> u32 {
        self.chained_postings[self.posting_id].freq()
    }
}

impl<'a> Postings for ChainedPostings<'a> {
    
    fn next(&mut self,) -> bool {
        if self.posting_id == self.chained_postings.len() {
            return false;
        }
        while !self.chained_postings[self.posting_id].next() {
            self.posting_id += 1;
            if self.posting_id == self.chained_postings.len() {
                return false;
            }   
        }
        return true
    }
    
    fn doc(&self,) -> DocId {
        self.chained_postings[self.posting_id].doc()
    }

    fn skip_next(&mut self, _target: DocId) -> SkipResult {
        // TODO implement.
        panic!("not implemented");
    }
    
    fn doc_freq(&self,) -> usize {
        self.doc_freq
    }
}
