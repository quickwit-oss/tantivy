#![allow(dead_code)]

use DocId;
use postings::{DocSet, HasLen, Postings};
use std::num::Wrapping;

const EMPTY_ARRAY: [u32; 0] = [];

/// Simulate a `Postings` objects from a `VecPostings`.
/// `VecPostings` only exist for testing purposes.
///
/// Term frequencies always return 1.
/// No positions are returned.
pub struct VecPostings {
    doc_ids: Vec<DocId>,
    cursor: Wrapping<usize>,
}

impl From<Vec<DocId>> for VecPostings {
    fn from(doc_ids: Vec<DocId>) -> VecPostings {
        VecPostings {
            doc_ids,
            cursor: Wrapping(usize::max_value()),
        }
    }
}

impl DocSet for VecPostings {
    fn advance(&mut self) -> bool {
        self.cursor += Wrapping(1);
        self.doc_ids.len() > self.cursor.0
    }

    fn doc(&self) -> DocId {
        self.doc_ids[self.cursor.0]
    }

    fn size_hint(&self) -> usize {
        self.len()
    }
}

impl HasLen for VecPostings {
    fn len(&self) -> usize {
        self.doc_ids.len()
    }
}

impl Postings for VecPostings {
    fn term_freq(&self) -> u32 {
        1u32
    }

    fn positions(&self) -> &[u32] {
        &EMPTY_ARRAY
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use DocId;
    use postings::{DocSet, Postings, SkipResult};

    #[test]
    pub fn test_vec_postings() {
        let doc_ids: Vec<DocId> = (0u32..1024u32).map(|e| e * 3).collect();
        let mut postings = VecPostings::from(doc_ids);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 0u32);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 3u32);
        assert_eq!(postings.term_freq(), 1u32);
        assert_eq!(postings.skip_next(14u32), SkipResult::OverStep);
        assert_eq!(postings.doc(), 15u32);
        assert_eq!(postings.skip_next(300u32), SkipResult::Reached);
        assert_eq!(postings.doc(), 300u32);
        assert_eq!(postings.skip_next(6000u32), SkipResult::End);
    }

}
