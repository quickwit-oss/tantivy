use std::borrow::Borrow;
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

pub trait HasLen {
    fn len(&self,) -> usize;
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
