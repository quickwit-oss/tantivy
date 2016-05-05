use DocId;
use postings::{Postings, SkipResult};

pub struct ChainedPostings<LeftPostings: Postings, RightPostings: Postings> {
    left: LeftPostings,
    right: RightPostings,
    right_offset: DocId,
    on_right: bool,
}

impl<LeftPostings: Postings, RightPostings: Postings> ChainedPostings<LeftPostings, RightPostings> {
    
    pub fn new(left: LeftPostings, right: RightPostings, right_offset: DocId) -> ChainedPostings<LeftPostings, RightPostings> {
        ChainedPostings {
            left: left,
            right: right,
            right_offset: right_offset,
            on_right: false,
        }
    }
    
}

impl<LeftPostings: Postings, RightPostings: Postings> Postings for ChainedPostings<LeftPostings, RightPostings> {
    
    fn next(&mut self,) -> bool {
        if self.on_right {
            self.right.next()
        }
        else {
            if self.left.next() {
                true
            }
            else {
                self.on_right = true;
                self.right.next()
            }
        }
    }
    
    fn doc(&self,) -> DocId {
        if self.on_right {
            self.right.doc() + self.right_offset
        }
        else {
            self.left.doc()
        }
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if self.on_right {
            self.right.skip_next(target)
        }
        else {
            self.left.skip_next(target)
        }
    }
}
