use postings::Postings;
use postings::SegmentPostings;
use postings::SkipResult;
use DocId;
pub struct OffsetPostings<'a> {
    underlying: SegmentPostings<'a>,
    offset: DocId,
}

impl<'a> OffsetPostings<'a> {
    pub fn new(underlying: SegmentPostings<'a>, offset: DocId) -> OffsetPostings {
        OffsetPostings {
            underlying: underlying,
            offset: offset,
        }
    }

    pub fn freq(&self,) -> u32 {
        self.underlying.freq()
    }
}

impl<'a> Postings for OffsetPostings<'a> {
    fn next(&mut self,) -> bool {
        self.underlying.next()
    }
    
    fn doc(&self,) -> DocId {
        self.underlying.doc() + self.offset
    }
    
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if target >= self.offset {
            SkipResult::OverStep
        }
        else {
            self.underlying.skip_next(target - self.offset)    
        }
    }
    
    fn doc_freq(&self,) -> usize {
        self.underlying.doc_freq()
    }
}