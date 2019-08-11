use crate::common::{BitSet, TinySet};
use crate::docset::{DocSet, SkipResult};
use crate::DocId;
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
    docs: BitSet,
    cursor_bucket: u32, //< index associated to the current tiny bitset
    cursor_tinybitset: TinySet,
    doc: u32,
}

impl BitSetDocSet {
    fn go_to_bucket(&mut self, bucket_addr: u32) {
        self.cursor_bucket = bucket_addr;
        self.cursor_tinybitset = self.docs.tinyset(bucket_addr);
    }
}

impl From<BitSet> for BitSetDocSet {
    fn from(docs: BitSet) -> BitSetDocSet {
        let first_tiny_bitset = if docs.max_value() == 0 {
            TinySet::empty()
        } else {
            docs.tinyset(0)
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
        if let Some(lower) = self.cursor_tinybitset.pop_lowest() {
            self.doc = (self.cursor_bucket as u32 * 64u32) | lower;
            return true;
        }
        if let Some(cursor_bucket) = self.docs.first_non_empty_bucket(self.cursor_bucket + 1) {
            self.go_to_bucket(cursor_bucket);
            let lower = self.cursor_tinybitset.pop_lowest().unwrap();
            self.doc = (cursor_bucket * 64u32) | lower;
            true
        } else {
            false
        }
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        // skip is required to advance.
        if !self.advance() {
            return SkipResult::End;
        }
        let target_bucket = target / 64u32;

        // Mask for all of the bits greater or equal
        // to our target document.
        match target_bucket.cmp(&self.cursor_bucket) {
            Ordering::Greater => {
                self.go_to_bucket(target_bucket);
                let greater_filter: TinySet = TinySet::range_greater_or_equal(target);
                self.cursor_tinybitset = self.cursor_tinybitset.intersect(greater_filter);
                if !self.advance() {
                    SkipResult::End
                } else if self.doc() == target {
                    SkipResult::Reached
                } else {
                    debug_assert!(self.doc() > target);
                    SkipResult::OverStep
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

    /// Returns half of the `max_doc`
    /// This is quite a terrible heuristic,
    /// but we don't have access to any better
    /// value.
    fn size_hint(&self) -> u32 {
        self.docs.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::BitSetDocSet;
    use crate::common::BitSet;
    use crate::docset::{DocSet, SkipResult};
    use crate::DocId;

    fn create_docbitset(docs: &[DocId], max_doc: DocId) -> BitSetDocSet {
        let mut docset = BitSet::with_max_value(max_doc);
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
