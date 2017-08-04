use std::borrow::Borrow;
use postings::docset::DocSet;

/// Postings (also called inverted list)
///
/// For a given term, it is the list of doc ids of the doc
/// containing the term. Optionally, for each document,
/// it may also give access to the term frequency
/// as well as the list of term positions.
///
/// Its main implementation is `SegmentPostings`,
/// but other implementations mocking `SegmentPostings` exist,
/// for merging segments or for testing.
pub trait Postings: DocSet {
    /// Returns the term frequency
    fn term_freq(&self) -> u32;
    /// Returns the list of positions of the term, expressed as a list of
    /// token ordinals.
    fn positions(&self) -> &[u32];
    /// Return the list of delta positions.
    ///
    /// Delta positions is simply the difference between
    /// two consecutive positions.
    /// The first delta position is the first position of the
    /// term in the document.
    ///
    /// For instance, if positions are `[7,13,17]`
    /// then delta positions `[7, 6, 4]`
    fn delta_positions(&self) -> &[u32];
}

impl<TPostings: Postings> Postings for Box<TPostings> {
    fn term_freq(&self) -> u32 {
        let unboxed: &TPostings = self.borrow();
        unboxed.term_freq()
    }

    fn positions(&self) -> &[u32] {
        let unboxed: &TPostings = self.borrow();
        unboxed.positions()
    }

    fn delta_positions(&self) -> &[u32] {
        let unboxed: &TPostings = self.borrow();
        unboxed.delta_positions()
    }
}

impl<'a, TPostings: Postings> Postings for &'a mut TPostings {
    fn term_freq(&self) -> u32 {
        let unref: &TPostings = *self;
        unref.term_freq()
    }

    fn positions(&self) -> &[u32] {
        let unref: &TPostings = *self;
        unref.positions()
    }

    fn delta_positions(&self) -> &[u32] {
        let unref: &TPostings = *self;
        unref.delta_positions()
    }
}
