use DocId;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::cmp::Ordering;


/// Expresses the outcome of a call to `DocSet`'s `.skip_next(...)`.
#[derive(PartialEq, Eq, Debug)]
pub enum SkipResult {
    /// target was in the docset
    Reached,
    /// target was not in the docset, skipping stopped as a greater element was found
    OverStep,
    /// the docset was entirely consumed without finding the target, nor any
    /// element greater than the target.
    End,
}


/// Represents an iterable set of sorted doc ids. 
pub trait DocSet {
    /// Goes to the next element.
    /// `.advance(...)` needs to be called a first time to point to the correct
    /// element.
    fn advance(&mut self,) -> bool;
    
    /// After skipping, position the iterator in such a way that `.doc()`
    /// will return a value greater than or equal to target.
    /// 
    /// SkipResult expresses whether the `target value` was reached, overstepped,
    /// or if the `DocSet` was entirely consumed without finding any value
    /// greater or equal to the `target`.  
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
    
    /// Returns the current document
    fn doc(&self,) -> DocId;
    
    /// Advances the cursor to the next document
    /// None is returned if the iterator has `DocSet` 
    /// has already been entirely consumed.  
    fn next(&mut self,) -> Option<DocId> {
        if self.advance() {
            Some(self.doc())
        }
        else {
            None
        }
    } 
}


impl<TDocSet: DocSet + ?Sized> DocSet for Box<TDocSet> {

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

    
