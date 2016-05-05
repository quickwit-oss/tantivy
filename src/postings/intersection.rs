use postings::Postings;
use postings::SkipResult;
use postings::SegmentPostings;
use std::cmp::Ordering;
use DocId;


pub struct IntersectionPostings<'a> {
    left: Box<Postings + 'a>,
    right: Box<Postings + 'a>,
    finished: bool, 
}

impl<'a> IntersectionPostings<'a> {
    
    fn from_pair(left: Box<Postings + 'a>, right: Box<Postings + 'a>) -> IntersectionPostings<'a> {
        IntersectionPostings {
            left: left,
            right: right,
            finished: false,
        }         
    }
    
    pub fn new(mut postings: Vec<Box<Postings + 'a>>) -> IntersectionPostings<'a> {
        let left = postings.pop().unwrap();
        let right;
        if postings.len() == 1 {
            right = postings.pop().unwrap();
        }
        else {
            right = Box::new(IntersectionPostings::new(postings));   
        }
        IntersectionPostings::from_pair(left, right)        
    }
}


impl<'a> Postings for IntersectionPostings<'a> {
    
    fn next(&mut self,) -> bool {
        if self.finished {
            return false;
        }
        
        if !self.left.next() {
            self.finished = true;
            return false;
        }
        if !self.right.next() {
            self.finished = true;
            return false;
        }
        loop {
            match self.left.doc().cmp(&self.right.doc()) {
                Ordering::Equal => {
                    return true;
                }
                Ordering::Less => {
                    if !self.left.next() {
                        self.finished = true;
                        return false;
                    }
                }
                Ordering::Greater => {
                    if !self.right.next() {
                        self.finished = true;
                        return false;
                    }
                }
            }
        }
    }
    
    fn doc(&self,) -> DocId {
        self.left.doc()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        loop {
            match self.doc().cmp(&target) {
                Ordering::Equal => {
                    return SkipResult::Reached;
                }
                Ordering::Greater => {
                    return SkipResult::OverStep;
                }
                Ordering::Less => {
                    //
                }
            }
            if !self.next() {
                return SkipResult::End;
            }
        }
    }
}




#[inline(never)]
pub fn intersection<'a>(postings: Vec<SegmentPostings<'a>>) -> IntersectionPostings<'a> {
    let boxed_postings: Vec<Box<Postings + 'a>> = postings
        .into_iter()
        .map(|postings| {
            let boxed_p: Box<Postings + 'a> = Box::new(postings);
            boxed_p
        })
        .collect();
    IntersectionPostings::new(boxed_postings)
    // let min_len = postings.iter()
    //                       .map(|v| v.len())
    //                       .min()
    //                       .unwrap();
    // let buffer: Vec<u32> = postings.pop().unwrap().0;
    // let mut output: Vec<u32> = Vec::with_capacity(min_len);
    // unsafe {
    //     output.set_len(min_len);
    // }
    // let mut pair = (output, buffer);
    // for posting in postings.iter() {
    //     pair = (pair.1, pair.0);
    //     let output_len = compression::intersection(posting.0.as_slice(),
    //                                                pair.0.as_slice(),
    //                                                pair.1.as_mut_slice());
    //     unsafe {
    //         pair.1.set_len(output_len);
    //     }
    // }
    // SegmentPostings(pair.1)
}