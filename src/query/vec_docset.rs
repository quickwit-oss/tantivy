#![allow(dead_code)]

use DocId;
use docset::DocSet;
use common::HasLen;
use std::num::Wrapping;

const EMPTY_ARRAY: [u32; 0] = [];

/// Simulate a `Postings` objects from a `VecPostings`.
/// `VecPostings` only exist for testing purposes.
///
/// Term frequencies always return 1.
/// No positions are returned.
pub struct VecDocSet {
    doc_ids: Vec<DocId>,
    cursor: Wrapping<usize>,
}

impl From<Vec<DocId>> for VecDocSet {
    fn from(doc_ids: Vec<DocId>) -> VecDocSet {
        VecDocSet {
            doc_ids,
            cursor: Wrapping(usize::max_value()),
        }
    }
}

impl DocSet for VecDocSet {
    fn advance(&mut self) -> bool {
        self.cursor += Wrapping(1);
        self.doc_ids.len() > self.cursor.0
    }

    fn doc(&self) -> DocId {
        self.doc_ids[self.cursor.0]
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }
}

impl HasLen for VecDocSet {
    fn len(&self) -> usize {
        self.doc_ids.len()
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use DocId;
    use docset::{DocSet, SkipResult};

    #[test]
    pub fn test_vec_postings() {
        let doc_ids: Vec<DocId> = (0u32..1024u32).map(|e| e * 3).collect();
        let mut postings = VecDocSet::from(doc_ids);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 0u32);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 3u32);
        assert_eq!(postings.skip_next(14u32), SkipResult::OverStep);
        assert_eq!(postings.doc(), 15u32);
        assert_eq!(postings.skip_next(300u32), SkipResult::Reached);
        assert_eq!(postings.doc(), 300u32);
        assert_eq!(postings.skip_next(6000u32), SkipResult::End);
    }

    #[test]
    pub fn test_fill_buffer() {
        let doc_ids: Vec<DocId> = (1u32..210u32).collect();
        let mut postings = VecDocSet::from(doc_ids);
        let mut buffer = vec![1000u32; 100];
        assert_eq!(postings.fill_buffer(&mut buffer[..]), 100);
        for i in 0u32..100u32 {
            assert_eq!(buffer[i as usize], i + 1);
        }
        assert_eq!(postings.fill_buffer(&mut buffer[..]), 100);
        for i in 0u32..100u32 {
            assert_eq!(buffer[i as usize], i + 101);
        }
        assert_eq!(postings.fill_buffer(&mut buffer[..]), 9);
    }

}
