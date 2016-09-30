use std::borrow::Borrow;
use postings::docset::DocSet;
use common::HasLen;



/// Postings (also called inverted list)
///
/// For a given term, it is the list of doc ids of the doc
/// containing the term. Optionally, for each document,
/// it may also give access to the term frequency
/// as well as the list of term positions.
/// 
/// Its main implementation is `SegmentPostings`,
/// but other implementations mocking SegmentPostings exist,
/// in order to help when merging segments or for testing.
pub trait Postings: DocSet {
    /// Returns the term frequency
    fn term_freq(&self,) -> u32;
    /// Returns the list of positions of the term, expressed as a list of
    /// token ordinals.
    fn positions(&self) -> &[u32];
}

impl<TPostings: Postings> Postings for Box<TPostings> {

    fn term_freq(&self,) -> u32 {
        let unboxed: &TPostings = self.borrow();
        unboxed.term_freq()
    }
    
    fn positions(&self) -> &[u32] {
        let unboxed: &TPostings = self.borrow();
        unboxed.positions()
    }

}

impl<'a, TPostings: Postings> Postings for &'a mut TPostings {

    fn term_freq(&self,) -> u32 {
        let unref: &TPostings = *self;
        unref.term_freq()
    }
   
    fn positions(&self) -> &[u32] {
        let unref: &TPostings = *self;
        unref.positions()
    }

}



impl<THasLen: HasLen> HasLen for Box<THasLen> {
     fn len(&self,) -> usize {
         let unboxed: &THasLen = self.borrow();
        unboxed.borrow().len()
     }
}

impl<'a> HasLen for &'a HasLen {
    fn len(&self,) -> usize {
        let unref: &HasLen = *self;
        unref.len()
    }
}
