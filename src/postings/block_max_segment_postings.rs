use crate::postings::{BlockMaxPostings, Postings, SegmentPostings};
use crate::{DocId, DocSet, SkipResult};

/// A wrapper over [`SegmentPostings`](./struct.SegmentPostings.html)
/// with max block frequencies.
pub struct BlockMaxSegmentPostings {
    postings: SegmentPostings,
    max_blocks: SegmentPostings,
    doc_with_max_term_freq: DocId,
    max_term_freq: u32,
}

impl BlockMaxSegmentPostings {
    /// Constructs a new segment postings with block-max information.
    pub fn new(
        postings: SegmentPostings,
        max_blocks: SegmentPostings,
        doc_with_max_term_freq: DocId,
        max_term_freq: u32,
    ) -> Self {
        Self {
            postings,
            max_blocks,
            doc_with_max_term_freq,
            max_term_freq,
        }
    }
}

impl DocSet for BlockMaxSegmentPostings {
    fn advance(&mut self) -> bool {
        self.postings.advance()
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.postings.skip_next(target)
    }
}

impl Postings for BlockMaxSegmentPostings {
    fn term_freq(&self) -> u32 {
        self.postings.term_freq()
    }
    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        self.postings.positions_with_offset(offset, output);
    }
    fn positions(&mut self, output: &mut Vec<u32>) {
        self.postings.positions(output);
    }
}

impl BlockMaxPostings for BlockMaxSegmentPostings {
    fn max_term_freq(&self) -> u32 {
        self.max_term_freq
    }
    fn block_max_term_freq(&mut self) -> u32 {
        if let SkipResult::End = self.max_blocks.skip_next(self.doc()) {
            panic!("Max blocks corrupted: reached end of max block");
        }
        self.max_blocks.term_freq()
    }
    fn max_doc(&self) -> DocId {
        self.doc_with_max_term_freq
    }
    fn block_max_doc(&self) -> DocId {
        self.max_blocks.doc()
    }
}
