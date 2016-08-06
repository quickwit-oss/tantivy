use postings::DocSet;
use std::cmp::Ordering;
use DocId;


pub struct IntersectionDocSet<'a> {
    left: Box<DocSet + 'a>,
    right: Box<DocSet + 'a>,
    finished: bool, 
}

impl<'a> IntersectionDocSet<'a> {
    
    fn from_pair(left: Box<DocSet + 'a>, right: Box<DocSet + 'a>) -> IntersectionDocSet<'a> {
        IntersectionDocSet {
            left: left,
            right: right,
            finished: false,
        }         
    }
    
    pub fn new(mut postings: Vec<Box<DocSet + 'a>>) -> IntersectionDocSet<'a> {
        let left = postings.pop().unwrap();
        let right;
        if postings.len() == 1 {
            right = postings.pop().unwrap();
        }
        else {
            right = Box::new(IntersectionDocSet::new(postings));   
        }
        IntersectionDocSet::from_pair(left, right)        
    }
}


impl<'a> DocSet for IntersectionDocSet<'a> {
    
    fn advance(&mut self,) -> bool {
        if self.finished {
            return false;
        }
        
        if !self.left.advance() {
            self.finished = true;
            return false;
        }
        if !self.right.advance() {
            self.finished = true;
            return false;
        }
        loop {
            match self.left.doc().cmp(&self.right.doc()) {
                Ordering::Equal => {
                    return true;
                }
                Ordering::Less => {
                    if !self.left.advance() {
                        self.finished = true;
                        return false;
                    }
                }
                Ordering::Greater => {
                    if !self.right.advance() {
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
}

#[inline(never)]
pub fn intersection<'a, TDocSet: DocSet + 'a>(postings: Vec<TDocSet>) -> IntersectionDocSet<'a> {
    let boxed_postings: Vec<Box<DocSet + 'a>> = postings
        .into_iter()
        .map(|postings| {
            let boxed_p: Box<DocSet + 'a> = Box::new(postings);
            boxed_p
        })
        .collect();
    IntersectionDocSet::new(boxed_postings)
}
