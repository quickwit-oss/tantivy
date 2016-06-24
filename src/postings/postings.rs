use DocId;
use std::borrow::Borrow;
use std::borrow::BorrowMut;

#[derive(PartialEq, Eq, Debug)]
pub enum SkipResult {
    Reached,
    OverStep,
    End,
}

pub trait Postings {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    fn next(&mut self,) -> bool;
    
    // after skipping position
    // the iterator in such a way that doc() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> SkipResult;
    
    fn doc(&self,) -> DocId;

    fn doc_freq(&self,) -> usize;
}

impl<TPostings: Postings> Postings for Box<TPostings> {
    fn next(&mut self,) -> bool {
        let unboxed: &mut TPostings = self.borrow_mut();
        unboxed.next()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unboxed: &mut TPostings = self.borrow_mut();
        unboxed.skip_next(target)
    }

    fn doc(&self,) -> DocId {
        let unboxed: &TPostings = self.borrow();
        unboxed.borrow().doc()
    }

    fn doc_freq(&self,) -> usize {
        let unboxed: &TPostings = self.borrow();
        unboxed.doc_freq()
    }
}

impl<'a, TPostings: Postings> Postings for &'a mut TPostings {
    fn next(&mut self,) -> bool {
        let unref: &mut TPostings = *self;
        unref.next()
    }
        
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unref: &mut TPostings = *self;
        unref.skip_next(target)
    }

    fn doc(&self,) -> DocId {
        let unref: &TPostings = *self;
        unref.doc()
    }

    
    fn doc_freq(&self,) -> usize {
        let unref: &TPostings = *self;
        unref.doc_freq()
    }
}
