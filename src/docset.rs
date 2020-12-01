use crate::fastfield::DeleteBitSet;
use crate::DocId;
use std::borrow::Borrow;
use std::borrow::BorrowMut;

/// Sentinel value returned when a DocSet has been entirely consumed.
///
/// This is not u32::MAX as one would have expected, due to the lack of SSE2 instructions
/// to compare [u32; 4].
pub const TERMINATED: DocId = std::i32::MAX as u32;

/// Represents an iterable set of sorted doc ids.
pub trait DocSet: Send {
    /// Goes to the next element.
    ///
    /// The DocId of the next element is returned.
    /// In other words we should always have :
    /// ```ignore
    /// let doc = docset.advance();
    /// assert_eq!(doc, docset.doc());
    /// ```
    ///
    /// If we reached the end of the DocSet, TERMINATED should be returned.
    ///
    /// Calling `.advance()` on a terminated DocSet should be supported, and TERMINATED should
    /// be returned.
    /// TODO Test existing docsets.
    fn advance(&mut self) -> DocId;

    /// Advances the DocSet forward until reaching the target, or going to the
    /// lowest DocId greater than the target.
    ///
    /// If the end of the DocSet is reached, TERMINATED is returned.
    ///
    /// Calling `.seek(target)` on a terminated DocSet is legal. Implementation
    /// of DocSet should support it.
    ///
    /// Calling `seek(TERMINATED)` is also legal and is the normal way to consume a DocSet.
    fn seek(&mut self, target: DocId) -> DocId {
        let mut doc = self.doc();
        debug_assert!(doc <= target);
        while doc < target {
            doc = self.advance();
        }
        doc
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

    /// Returns the number documents matching.
    /// Calling this method consumes the `DocSet`.
    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        let mut count = 0u32;
        let mut doc = self.doc();
        while doc != TERMINATED {
            if !delete_bitset.is_deleted(doc) {
                count += 1u32;
            }
            doc = self.advance();
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
        let mut doc = self.doc();
        while doc != TERMINATED {
            count += 1u32;
            doc = self.advance();
        }
        count
    }
}

impl<'a> DocSet for &'a mut dyn DocSet {
    fn advance(&mut self) -> u32 {
        (**self).advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        (**self).seek(target)
    }

    fn doc(&self) -> u32 {
        (**self).doc()
    }

    fn size_hint(&self) -> u32 {
        (**self).size_hint()
    }

    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        (**self).count(delete_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        (**self).count_including_deleted()
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

    fn count(&mut self, delete_bitset: &DeleteBitSet) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count(delete_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count_including_deleted()
    }
}
