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
    fn advance(&mut self) -> bool;

    /// After skipping, position the iterator in such a way that `.doc()`
    /// will return a value greater than or equal to target.
    ///
    /// SkipResult expresses whether the `target value` was reached, overstepped,
    /// or if the `DocSet` was entirely consumed without finding any value
    /// greater or equal to the `target`.
    ///
    /// WARNING: Calling skip always advances the docset.
    /// More specifically, if the docset is already positionned on the target
    /// skipping will advance to the next position and return SkipResult::Overstep.
    ///
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if !self.advance() {
            return SkipResult::End;
        }
        loop {
            match self.doc().cmp(&target) {
                Ordering::Less => {
                    if !self.advance() {
                        return SkipResult::End;
                    }
                }
                Ordering::Equal => return SkipResult::Reached,
                Ordering::Greater => return SkipResult::OverStep,
            }
        }
    }

    /// Fills a given mutable buffer with the next doc ids from the
    /// `DocSet`
    ///
    /// If that many `DocId`s are available, the method should
    /// fill the entire buffer and return the length of the buffer.
    ///
    /// If we reach the end of the `DocSet` before filling
    /// it entirely, then the buffer is filled up to this point, and
    /// return value is the number of elements that were filled.
    ///
    /// # Warning
    ///
    /// This method is only here for specific high-performance
    /// use case where batching. The normal way to
    /// go through the `DocId`'s is to call `.advance()`.
    fn fill_buffer(&mut self, buffer: &mut [DocId]) -> usize {
        for (i, buffer_val) in buffer.iter_mut().enumerate() {
            if self.advance() {
                *buffer_val = self.doc();
            } else {
                return i;
            }
        }
        buffer.len()
    }

    /// Returns the current document
    fn doc(&self) -> DocId;

    /// Advances the cursor to the next document
    /// None is returned if the iterator has `DocSet`
    /// has already been entirely consumed.
    fn next(&mut self) -> Option<DocId> {
        if self.advance() {
            Some(self.doc())
        } else {
            None
        }
    }

    /// Returns a best-effort hint of the
    /// length of the docset.
    fn size_hint(&self) -> u32;
}

impl<TDocSet: DocSet + ?Sized> DocSet for Box<TDocSet> {
    fn advance(&mut self) -> bool {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.skip_next(target)
    }

    fn doc(&self) -> DocId {
        let unboxed: &TDocSet = self.borrow();
        unboxed.doc()
    }

    fn size_hint(&self) -> u32 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.size_hint()
    }
}

impl<'a, TDocSet: DocSet> DocSet for &'a mut TDocSet {
    fn advance(&mut self) -> bool {
        let unref: &mut TDocSet = *self;
        unref.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let unref: &mut TDocSet = *self;
        unref.skip_next(target)
    }

    fn doc(&self) -> DocId {
        let unref: &TDocSet = *self;
        unref.doc()
    }

    fn size_hint(&self) -> u32 {
        let unref: &TDocSet = *self;
        unref.size_hint()
    }
}
