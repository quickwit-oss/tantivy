use std::borrow::Borrow;
use std::borrow::BorrowMut;
use postings::docset::DocSet;


pub trait Postings: DocSet {
    fn term_freq(&self,) -> u32;
}

impl<TPostings: Postings> Postings for Box<TPostings> {

    fn term_freq(&self,) -> u32 {
        let unboxed: &TPostings = self.borrow();
        unboxed.term_freq()
    }
}

impl<'a, TPostings: Postings> Postings for &'a mut TPostings {
   
    fn term_freq(&self,) -> u32 {
        let unref: &TPostings = *self;
        unref.term_freq()
    }
}
