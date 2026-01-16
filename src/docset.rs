use std::borrow::{Borrow, BorrowMut};

use common::BitSet;

use crate::fastfield::AliveBitSet;
use crate::DocId;

/// Sentinel value returned when a [`DocSet`] has been entirely consumed.
///
/// This is not `u32::MAX` as one would have expected, due to the lack of SSE2 instructions
/// to compare `[u32; 4]`.
pub const TERMINATED: DocId = i32::MAX as u32;

/// The collect_block method on `SegmentCollector` uses a buffer of this size.
/// Passed results to `collect_block` will not exceed this size and will be
/// exactly this size as long as we can fill the buffer.
pub const COLLECT_BLOCK_BUFFER_LEN: usize = 64;

/// Represents an iterable set of sorted doc ids.
pub trait DocSet: Send {
    /// Goes to the next element.
    ///
    /// The DocId of the next element is returned.
    /// In other words we should always have :
    /// ```compile_fail
    /// let doc = docset.advance();
    /// assert_eq!(doc, docset.doc());
    /// ```
    ///
    /// If we reached the end of the `DocSet`, [`TERMINATED`] should be returned.
    ///
    /// Calling `.advance()` on a terminated `DocSet` should be supported, and [`TERMINATED`] should
    /// be returned.
    fn advance(&mut self) -> DocId;

    /// Advances the `DocSet` forward until reaching the target, or going to the
    /// lowest [`DocId`] greater than the target.
    ///
    /// If the end of the `DocSet` is reached, [`TERMINATED`] is returned.
    ///
    /// Calling `.seek(target)` on a terminated `DocSet` is legal. Implementation
    /// of `DocSet` should support it.
    ///
    /// Calling `seek(TERMINATED)` is also legal and is the normal way to consume a `DocSet`.
    ///
    /// `target` has to be larger or equal to `.doc()` when calling `seek`.
    fn seek(&mut self, target: DocId) -> DocId {
        let mut doc = self.doc();
        debug_assert!(doc <= target);
        while doc < target {
            doc = self.advance();
        }
        doc
    }

    /// Seeks to the target if possible and returns true if the target is in the DocSet.
    ///
    /// DocSets that already have an efficient `seek` method don't need to implement
    /// `seek_into_the_danger_zone`. All wrapper DocSets should forward
    /// `seek_into_the_danger_zone` to the underlying DocSet.
    ///
    /// ## API Behaviour
    /// If `seek_into_the_danger_zone` is returning true, a call to `doc()` has to return target.
    /// If `seek_into_the_danger_zone` is returning false, a call to `doc()` may return any doc
    /// between the last doc that matched and target or a doc that is a valid next hit after
    /// target. The DocSet is considered to be in an invalid state until
    /// `seek_into_the_danger_zone` returns true again.
    ///
    /// `target` needs to be equal or larger than `doc` when in a valid state.
    ///
    /// Consecutive calls are not allowed to have decreasing `target` values.
    ///
    /// # Warning
    /// This is an advanced API used by intersection. The API contract is tricky, avoid using it.
    fn seek_into_the_danger_zone(&mut self, target: DocId) -> bool {
        let current_doc = self.doc();
        if current_doc < target {
            self.seek(target);
        }
        self.doc() == target
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
    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
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

    // comment on the size of the bitset
    fn fill_bitset(&mut self, bitset: &mut BitSet) {
        let mut doc = self.doc();
        while doc != TERMINATED {
            bitset.insert(doc);
            doc = self.advance();
        }
    }

    /// Returns the current document
    /// Right after creating a new `DocSet`, the docset points to the first document.
    ///
    /// If the `DocSet` is empty, `.doc()` should return [`TERMINATED`].
    fn doc(&self) -> DocId;

    /// Returns a best-effort hint of the
    /// length of the docset.
    fn size_hint(&self) -> u32;

    /// Returns a best-effort hint of the cost to consume the entire docset.
    ///
    /// Consuming means calling advance until [`TERMINATED`] is returned.
    /// The cost should be relative to the cost of driving a Term query,
    /// which would be the number of documents in the DocSet.
    ///
    /// By default this returns `size_hint()`.
    ///
    /// DocSets may have vastly different cost depending on their type,
    /// e.g. an intersection with 10 hits is much cheaper than
    /// a phrase search with 10 hits, since it needs to load positions.
    ///
    /// ### Future Work
    /// We may want to differentiate `DocSet` costs more more granular, e.g.
    /// creation_cost, advance_cost, seek_cost on to get a good estimation
    /// what query types to choose.
    fn cost(&self) -> u64 {
        self.size_hint() as u64
    }

    /// Returns the number documents matching.
    /// Calling this method consumes the `DocSet`.
    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        let mut count = 0u32;
        let mut doc = self.doc();
        while doc != TERMINATED {
            if alive_bitset.is_alive(doc) {
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

impl DocSet for &mut dyn DocSet {
    fn advance(&mut self) -> u32 {
        (**self).advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        (**self).seek(target)
    }

    fn seek_into_the_danger_zone(&mut self, target: DocId) -> bool {
        (**self).seek_into_the_danger_zone(target)
    }

    fn doc(&self) -> u32 {
        (**self).doc()
    }

    fn size_hint(&self) -> u32 {
        (**self).size_hint()
    }

    fn cost(&self) -> u64 {
        (**self).cost()
    }

    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        (**self).count(alive_bitset)
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

    fn seek_into_the_danger_zone(&mut self, target: DocId) -> bool {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.seek_into_the_danger_zone(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        let unboxed: &TDocSet = self.borrow();
        unboxed.doc()
    }

    fn size_hint(&self) -> u32 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.size_hint()
    }

    fn cost(&self) -> u64 {
        let unboxed: &TDocSet = self.borrow();
        unboxed.cost()
    }

    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count(alive_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        let unboxed: &mut TDocSet = self.borrow_mut();
        unboxed.count_including_deleted()
    }
}
