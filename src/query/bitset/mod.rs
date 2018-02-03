use common::{DocBitSet, TinySet};
use DocId;
use postings::DocSet;
use postings::SkipResult;
use std::cmp::Ordering;

/// A `BitSetDocSet` makes it possible to iterate through a bitset as if it was a `DocSet`.
///
/// # Implementation detail
///
/// Skipping is relatively fast here as we can directly point to the
/// right tiny bitset bucket.
///
/// TODO: Consider implementing a `BitTreeSet` in order to advance faster
/// when the bitset is sparse
pub struct BitSetDocSet {
    docs: DocBitSet,
    cursor_bucket: usize, //< index associated to the current tiny bitset
    cursor_tinybitset: TinySet,
    doc: u32,
}

impl BitSetDocSet {
    #[inline(always)]
    fn check_validity(&self) {
        if cfg!(test) {
            let original_bucket = self.docs.tiny_bitset(self.cursor_bucket);
            debug_assert_eq!(
                original_bucket.intersect(self.cursor_tinybitset),
                self.cursor_tinybitset
            );
            debug_assert!(self.cursor_bucket >= self.doc() as usize / 64);
        }
    }

    fn go_to_bucket(&mut self, bucket_addr: usize) {
        self.cursor_bucket = bucket_addr;
        self.cursor_tinybitset = self.docs.tiny_bitset(bucket_addr);
    }
}

impl From<DocBitSet> for BitSetDocSet {
    fn from(docs: DocBitSet) -> BitSetDocSet {
        let first_tiny_bitset = if docs.num_tiny_bitsets() == 0 {
            TinySet::empty()
        } else {
            docs.tiny_bitset(0)
        };
        BitSetDocSet {
            docs,
            cursor_bucket: 0,
            cursor_tinybitset: first_tiny_bitset,
            doc: 0u32,
        }
    }
}

impl DocSet for BitSetDocSet {
    fn advance(&mut self) -> bool {
        self.check_validity();
        loop {
            if let Some(lower) = self.cursor_tinybitset.pop_lowest() {
                self.doc = (self.cursor_bucket as u32 * 64u32) | lower;
                return true;
            } else {
                if self.cursor_bucket < self.docs.num_tiny_bitsets() - 1 {
                    let inc_bucket = self.cursor_bucket + 1;
                    self.go_to_bucket(inc_bucket);
                } else {
                    return false;
                }
            }
        }
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.check_validity();
        // skip is required to advance.
        if !self.advance() {
            return SkipResult::End;
        }
        let target_bucket = (target / 64u32) as usize;

        // Mask for all of the bits greater or equal
        // to our target document.
        match target_bucket.cmp(&self.cursor_bucket) {
            Ordering::Greater => {
                self.go_to_bucket(target_bucket);
                let greater_filter: TinySet = TinySet::range_greater_or_equal(target);
                self.cursor_tinybitset.intersect_update(greater_filter);
                if !self.advance() {
                    SkipResult::End
                } else {
                    if self.doc() == target {
                        SkipResult::Reached
                    } else {
                        debug_assert!(self.doc() > target);
                        SkipResult::OverStep
                    }
                }
            }
            Ordering::Equal => loop {
                match self.doc().cmp(&target) {
                    Ordering::Less => {
                        if !self.advance() {
                            return SkipResult::End;
                        }
                    }
                    Ordering::Equal => {
                        return SkipResult::Reached;
                    }
                    Ordering::Greater => {
                        debug_assert!(self.doc() > target);
                        return SkipResult::OverStep;
                    }
                }
            },
            Ordering::Less => {
                debug_assert!(self.doc() > target);
                SkipResult::OverStep
            }
        }
    }

    /// Returns the current document
    fn doc(&self) -> DocId {
        self.doc
    }

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

    /// Returns half of the `max_doc`
    /// This is quite a terrible heuristic,
    /// but we don't have access to any better
    /// value.
    fn size_hint(&self) -> u32 {
        self.docs.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use DocId;
    use common::DocBitSet;
    use postings::{DocSet, SkipResult};
    use super::BitSetDocSet;

    fn create_docbitset(docs: &[DocId], max_doc: DocId) -> BitSetDocSet {
        let mut docset = DocBitSet::with_maxdoc(max_doc);
        for &doc in docs {
            docset.insert(doc);
        }
        BitSetDocSet::from(docset)
    }

    fn test_go_through_sequential(docs: &[DocId]) {
        let mut docset = create_docbitset(docs, 1_000u32);
        for &doc in docs {
            assert!(docset.advance());
            assert_eq!(doc, docset.doc());
        }
        assert!(!docset.advance());
        assert!(!docset.advance());
    }

    #[test]
    fn test_docbitset_sequential() {
        test_go_through_sequential(&[]);
        test_go_through_sequential(&[1, 2, 3]);
        test_go_through_sequential(&[1, 2, 3, 4, 5, 63, 64, 65]);
        test_go_through_sequential(&[63, 64, 65]);
        test_go_through_sequential(&[1, 2, 3, 4, 95, 96, 97, 98, 99]);
    }

    #[test]
    fn test_docbitset_skip() {
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112], 10_000);
            assert_eq!(docset.skip_next(7), SkipResult::Reached);
            assert_eq!(docset.doc(), 7);
            assert!(docset.advance(), 7);
            assert_eq!(docset.doc(), 5112);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112], 10_000);
            assert_eq!(docset.skip_next(3), SkipResult::OverStep);
            assert_eq!(docset.doc(), 5);
            assert!(docset.advance());
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.skip_next(5112), SkipResult::Reached);
            assert_eq!(docset.doc(), 5112);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.skip_next(5113), SkipResult::End);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.skip_next(5111), SkipResult::OverStep);
            assert_eq!(docset.doc(), 5112);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5500, 6666], 10_000);
            assert_eq!(docset.skip_next(5112), SkipResult::Reached);
            assert_eq!(docset.doc(), 5112);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 5500);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 6666);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5500, 6666], 10_000);
            assert_eq!(docset.skip_next(5111), SkipResult::OverStep);
            assert_eq!(docset.doc(), 5112);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 5500);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 6666);
            assert!(!docset.advance());
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5513, 6666], 10_000);
            assert_eq!(docset.skip_next(5111), SkipResult::OverStep);
            assert_eq!(docset.doc(), 5112);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 5513);
            assert!(docset.advance());
            assert_eq!(docset.doc(), 6666);
            assert!(!docset.advance());
        }
    }

}
