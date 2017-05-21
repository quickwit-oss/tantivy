use compression::{NUM_DOCS_PER_BLOCK, BlockDecoder, VIntDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen, SkipResult};
use std::cmp;
use fastfield::DeleteBitSet;


const EMPTY_DATA: [u8; 0] = [0u8; 0];


/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings<'a> {
    block_cursor: BlockSegmentPostings<'a>,
    cur: usize,
    delete_bitset: DeleteBitSet,
}

impl<'a> SegmentPostings<'a> {


    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub fn from_block_postings(
            segment_block_postings: BlockSegmentPostings<'a>,
            delete_bitset: DeleteBitSet) -> SegmentPostings<'a> {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: NUM_DOCS_PER_BLOCK,  // cursor within the block
            delete_bitset: delete_bitset,
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> SegmentPostings<'static> {
        let empty_block_cursor = BlockSegmentPostings::empty();
        SegmentPostings {
            block_cursor: empty_block_cursor,
            delete_bitset: DeleteBitSet::empty(),
            cur: NUM_DOCS_PER_BLOCK,
        }
    }
}


impl<'a> DocSet for SegmentPostings<'a> {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> bool {
        loop {
            self.cur += 1;
            if self.cur >= self.block_cursor.block_len() {
                self.cur = 0;
                if !self.block_cursor.advance() {
                    self.cur = NUM_DOCS_PER_BLOCK;
                    return false;
                }
            }
            if !self.delete_bitset.is_deleted(self.doc()) {
                return true;
            }
        }
    }


    fn skip_next(&mut self, target: DocId) -> SkipResult {
        if !self.advance() {
            return SkipResult::End;
        }

        // skip blocks until one that might contain the target
        loop {
            // check if we need to go to the next block
            let (current_doc, last_doc_in_block) = {
                let block_docs = self.block_cursor.docs();
                (block_docs[self.cur], block_docs[block_docs.len() - 1])
            };
            if target > last_doc_in_block {
                if !self.block_cursor.advance() {
                    return SkipResult::End;
                }
                self.cur = 0;
            } else {
                if target < current_doc {
                    // We've overpassed the target after the first `advance` call
                    // or we're at the beginning of a block.
                    // Either way, we're on the first `DocId` greater than `target`
                    return SkipResult::OverStep;
                }
                break;
            }
        }
        {
            // we're in the right block now, start with an exponential search
            let block_docs = self.block_cursor.docs();
            let block_len = block_docs.len();
            
            debug_assert!(target >= block_docs[self.cur]);
            debug_assert!(target <= block_docs[block_len - 1]);

            let mut start = self.cur;
            let mut end = block_len;
            let mut count = 1;
            loop {
                let new = start + count;
                if new < end && block_docs[new] < target {
                    start = new;
                    count *= 2;
                } else {
                    break;
                }
            }
            end = cmp::min(start + count, end);

            // now do a binary search
            let mut count = end - start;
            while count > 0 {
                let step = count / 2;
                let mid = start + step;
                let doc = block_docs[mid];
                if doc < target {
                    start = mid + 1;
                    count -= step + 1;
                } else {
                    count = step;
                }
            }

            // `doc` is now >= `target`
            let doc = block_docs[start];
            self.cur = start;

            if !self.delete_bitset.is_deleted(doc) {
                if doc == target {
                    return SkipResult::Reached;
                } else {
                    return SkipResult::OverStep;
                }
            }
        }
        if self.advance() {
            SkipResult::OverStep
        } else {
            SkipResult::End
        }
    }


    #[inline]
    fn doc(&self) -> DocId {
        let docs = self.block_cursor.docs();
        assert!(self.cur < docs.len(), "Have you forgotten to call `.advance()` at least once before calling .doc().");
        docs[self.cur]
    }
}

impl<'a> HasLen for SegmentPostings<'a> {
    fn len(&self) -> usize {
        self.block_cursor.len
    }
}

impl<'a> Postings for SegmentPostings<'a> {
    fn term_freq(&self) -> u32 {
        self.block_cursor.freq_handler().freq(self.cur)
    }

    fn positions(&self) -> &[u32] {
        self.block_cursor.freq_handler().positions(self.cur)
    }
}




pub struct BlockSegmentPostings<'a> {
    block_decoder: BlockDecoder,
    len: usize,
    doc_offset: DocId,
    num_binpacked_blocks: usize,
    num_vint_docs: usize,
    remaining_data: &'a [u8],
    freq_handler: FreqHandler,
}

impl<'a> BlockSegmentPostings<'a> {

    pub fn from_data(len: usize, data: &'a [u8], freq_handler: FreqHandler) -> BlockSegmentPostings<'a> {
        let num_binpacked_blocks: usize = (len as usize) / NUM_DOCS_PER_BLOCK;
        let num_vint_docs = (len as usize) - NUM_DOCS_PER_BLOCK * num_binpacked_blocks;
        BlockSegmentPostings {
            num_binpacked_blocks: num_binpacked_blocks,
            num_vint_docs: num_vint_docs,
            block_decoder: BlockDecoder::new(),
            freq_handler: freq_handler,
            remaining_data: data,
            doc_offset: 0,
            len: len,
        }
    }

    pub fn reset(&mut self, len: usize, data: &'a [u8]) {
        let num_binpacked_blocks: usize = (len as usize) / NUM_DOCS_PER_BLOCK;
        let num_vint_docs = (len as usize) - NUM_DOCS_PER_BLOCK * num_binpacked_blocks;
        self.num_binpacked_blocks = num_binpacked_blocks;
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = data;
        self.doc_offset = 0;
        self.len = len;
    }

    /// Returns the array of docs in the current block.
    #[inline]
    pub fn docs(&self) -> &[DocId] {
        self.block_decoder.output_array()
    }

    #[inline]
    pub fn block_len(&self) -> usize {
        self.block_decoder.output_len
    }

    #[inline]
    pub fn freq_handler(&self) -> &FreqHandler {
        &self.freq_handler
    }

    pub fn advance(&mut self) -> bool {
        if self.num_binpacked_blocks > 0 {
            self.remaining_data = self.block_decoder.uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_handler.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
            self.num_binpacked_blocks -= 1;
            true
        }
        else {
            if self.num_vint_docs > 0 {
                self.remaining_data = self.block_decoder.uncompress_vint_sorted(self.remaining_data, self.doc_offset, self.num_vint_docs);
                self.freq_handler.read_freq_vint(self.remaining_data, self.num_vint_docs);
                self.num_vint_docs = 0;
                true
            }
            else {
                false
            }
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings<'static> {
        BlockSegmentPostings {
            num_binpacked_blocks: 0,
            num_vint_docs: 0,
            block_decoder: BlockDecoder::new(),
            freq_handler: FreqHandler::new_without_freq(),
            remaining_data:  &EMPTY_DATA,
            doc_offset: 0,
            len: 0,
        }
    }

}

#[cfg(test)]
mod tests {

    use DocSet;
    use super::SegmentPostings;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert!(!postings.advance());
        assert!(!postings.advance());
    }
}
