use DocId;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::cmp::Ordering;

#[derive(PartialEq, Eq, Debug)]
pub enum SkipResult {
    Reached,
    OverStep,
    End,
}


pub trait DocSet {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    fn advance(&mut self,) -> bool;
    
    // after skipping position
    // the iterator in such a way that doc() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        loop {
            match self.doc().cmp(&target) {
                Ordering::Less => {
                    if !self.advance() {
                        return SkipResult::End;
                    }
                },
                Ordering::Equal => { return SkipResult::Reached },
                Ordering::Greater => { return SkipResult::OverStep },
            }
        }
    }

    fn doc(&self,) -> DocId;
    
    fn next(&mut self,) -> Option<DocId> {
        if self.advance() {
            Some(self.doc())
        }
        else {
            None
        }
    } 
}


impl<TDocSet: DocSet> DocSet for Box<TDocSet> {

    fn advance(&mut self,) -> bool {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.skip_next(target)
    }

    fn doc(&self,) -> DocId {
        let unboxed: &TDocSet = self.borrow();
        unboxed.doc()
    }
}

impl<'a, TDocSet: DocSet> DocSet for &'a mut TDocSet {
   
    fn advance(&mut self,) -> bool {
        let unref: &mut TDocSet = *self;
        unref.advance()
    }
        
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unref: &mut TDocSet = *self;
        unref.skip_next(target)
    }

    fn doc(&self,) -> DocId {
        let unref: &TDocSet = *self;
        unref.doc()
    }
}

    