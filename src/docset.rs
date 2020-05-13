use crate::common::BitSet;
use crate::fastfield::DeleteBitSet;
use crate::DocId;
use std::borrow::Borrow;
use std::borrow::BorrowMut;

pub const TERMINATED: DocId = i32::MAX as u32;

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
    ///
    /// Calling `.advance()` on a terminated DocSet is illegal.
    fn advance(&mut self) -> DocId;

    /// Advances the DocSet forward until reaching, or going to the
    /// lowest DocId greater than the target.
    ///
    /// If the end of the DocSet is reached, the DocSet will be positionned on TERMINATED.
    ///
    /// Calling `.seek(target)` on a terminated DocSet is legal. Implementation
    /// of DocSet should support it.
    fn seek(&mut self, target: DocId) -> DocId {
        while self.doc() < target {
            self.advance();
        }
        self.doc()
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
        if self.doc() == TERMINATED {
            return 0;
        }
        for (i, buffer_val) in buffer.iter_mut().enumerate() {
            *buffer_val = self.doc();
            if self.advance() == TERMINATED {
                return i + 1;
            }
        }
        buffer.len()
    }

    /// Returns the current document
    /// Right after creating a new DocSet, the docset points to the first document.
    ///
    /// If the DocSet is empty, .doc() should return `TERMINATED`.
    fn doc(&self) -> DocId;

    /// Returns a best-effort hint of the
    /// length of the docset.
    fn size_hint(&self) -> u32;

    /// Appends all docs to a `bitset`.
    fn append_to_bitset(&mut self, bitset: &mut BitSet) {
        while self.doc() != TERMINATED {
            bitset.insert(self.doc());
            self.advance();
        }
    }

    /// Returns the number documents matching.
    /// Calling this method consumes the `DocSet`.
    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        let mut count = 0u32;
        while self.doc() != TERMINATED {
            if !delete_bitset.is_deleted(self.doc()) {
                count += 1u32;
            }
            self.advance();
        }
        count
    }

    /// Returns the count of documents, deleted or not.
    /// Calling this method consumes the `DocSet`.
    ///
    /// Of course, the result is an upper bound of the result
    /// given by `count()`.
    fn count_including_deleted(&mut self) -> u32 {
        let mut count = 0u32;
        while self.doc() != TERMINATED {
            count += 1u32;
            self.advance();
        }
        count
    }
}

impl<TDocSet: DocSet + ?Sized> DocSet for Box<TDocSet> {
    fn advance(&mut self) -> DocId {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.seek(target)
    }

    fn doc(&self) -> DocId {
        let unboxed: &TDocSet = self.borrow();
        unboxed.doc()
    }

    fn size_hint(&self) -> u32 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.size_hint()
    }

    fn append_to_bitset(&mut self, bitset: &mut BitSet) {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.append_to_bitset(bitset);
    }

    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count(delete_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count_including_deleted()
    }
}
