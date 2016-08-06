use DocId;
use postings::Postings;
use postings::OffsetPostings;
use postings::DocSet;
use postings::HasLen;

pub struct ChainedPostings<'a> {
    chained_postings: Vec<OffsetPostings<'a>>,
    posting_id: usize,
    len: usize,
}

impl<'a> ChainedPostings<'a> {
    
    pub fn new(chained_postings: Vec<OffsetPostings<'a>>) -> ChainedPostings {
        let len: usize = chained_postings
            .iter()
            .map(|segment_postings| segment_postings.len())
            .fold(0, |sum, addition| sum + addition);
        ChainedPostings {
            chained_postings: chained_postings,
            posting_id: 0,
            len: len,
        }
    }
}

impl<'a> DocSet for ChainedPostings<'a> {

    fn advance(&mut self,) -> bool {
        if self.posting_id == self.chained_postings.len() {
            return false;
        }
        while !self.chained_postings[self.posting_id].advance() {
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
}

impl<'a> HasLen for ChainedPostings<'a> {
    fn len(&self,) -> usize {
        self.len
    }
}

impl<'a> Postings for ChainedPostings<'a> {
    
    fn term_freq(&self,) -> u32 {
        self.chained_postings[self.posting_id].term_freq()
    }
    
}
