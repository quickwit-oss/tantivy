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
    cursor_tinybitset: u64,
    doc: u32
}

impl From<DocBitSet> for BitSetDocSet {
    fn from(docs: DocBitSet) -> BitSetDocSet {
        let first_tiny_bitset = 
            if docs.num_tiny_bitsets() == 0 {
                0u64
            } else {
                docs.tiny_bitset(0) as u64
            };
        BitSetDocSet {
            docs,
            cursor_bucket: 0,
            cursor_tinybitset: first_tiny_bitset,
            doc: 0u32
        }
    }
}

impl DocSet for BitSetDocSet {
    fn advance(&mut self) -> bool {
        loop {
            if let Some(lower) = self.cursor_tinybitset.pop_lowest() {
                self.doc = (self.cursor_bucket as u32 * 64u32) | lower;
                return true;
            } else {
                if self.cursor_bucket < self.docs.num_tiny_bitsets() - 1 {
                    self.cursor_bucket += 1;
                    self.cursor_tinybitset = self.docs.tiny_bitset(self.cursor_bucket);
                } else {
                    return false;
                }
            }

        }
    }

    
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        // skip is required to advance.
        if !self.advance() {
            return SkipResult::End;
        }
        let target_bucket = (target / 64u32) as usize;
        
        // Mask for all of the bits greater or equal
        // to our target document.
        match target_bucket.cmp(&self.cursor_bucket) {
            Ordering::Less => {
                self.cursor_bucket = target_bucket;
                self.cursor_tinybitset = self.docs.tiny_bitset(target_bucket);
                let greater: u64 = <u64 as TinySet>::range_greater_or_equal(target % 64);
                self.cursor_tinybitset.intersect(greater);
                if !self.advance() {
                    SkipResult::End
                } else {
                    if self.doc() == target {
                        SkipResult::Reached
                    } else {
                        SkipResult::OverStep
                    }
                }
            }
            Ordering::Equal => {
                loop {
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
                            return SkipResult::OverStep;
                        }
                    }
                }
            }
            Ordering::Greater => SkipResult::OverStep
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
    use postings::{SkipResult, DocSet};
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
        test_go_through_sequential(&[1,2,3]);
        test_go_through_sequential(&[1,2,3,4,5,63,64,65]);
        test_go_through_sequential(&[63,64,65]);
        test_go_through_sequential(&[1,2,3,4,95,96,97,98,99]);
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
    }

}