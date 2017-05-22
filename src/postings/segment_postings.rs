use compression::{NUM_DOCS_PER_BLOCK, BlockDecoder, VIntDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen, SkipResult};
use std::cmp;
use fastfield::DeleteBitSet;
use fst::Streamer;


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
    pub fn from_block_postings(segment_block_postings: BlockSegmentPostings<'a>,
                               delete_bitset: DeleteBitSet)
                               -> SegmentPostings<'a> {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: NUM_DOCS_PER_BLOCK, // cursor within the block
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

    fn size_hint(&self) -> usize {
        self.len()
    }

    #[inline]
    fn doc(&self) -> DocId {
        let docs = self.block_cursor.docs();
        assert!(self.cur < docs.len(),
                "Have you forgotten to call `.advance()` at least once before calling .doc().");
        docs[self.cur]
    }
}

impl<'a> HasLen for SegmentPostings<'a> {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq()
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

/// `BlockSegmentPostings` is a cursor iterating over blocks
/// of documents.
///
/// # Warning
///
/// While it is useful for some very specific high-performance
/// use cases, you should prefer using `SegmentPostings` for most usage.
pub struct BlockSegmentPostings<'a> {
    block_decoder: BlockDecoder,
    doc_freq: usize,
    doc_offset: DocId,
    num_binpacked_blocks: usize,
    num_vint_docs: usize,
    remaining_data: &'a [u8],
    freq_handler: FreqHandler,
}

impl<'a> BlockSegmentPostings<'a> {
    pub(crate) fn from_data(doc_freq: usize,
                            data: &'a [u8],
                            freq_handler: FreqHandler)
                            -> BlockSegmentPostings<'a> {
        let num_binpacked_blocks: usize = (doc_freq as usize) / NUM_DOCS_PER_BLOCK;
        let num_vint_docs = (doc_freq as usize) - NUM_DOCS_PER_BLOCK * num_binpacked_blocks;
        BlockSegmentPostings {
            num_binpacked_blocks: num_binpacked_blocks,
            num_vint_docs: num_vint_docs,
            block_decoder: BlockDecoder::new(),
            freq_handler: freq_handler,
            remaining_data: data,
            doc_offset: 0,
            doc_freq: doc_freq,
        }
    }

    // Resets the block segment postings on another position
    // in the postings file.
    //
    // This is useful for enumerating through a list of terms,
    // and consuming the associated posting lists while avoiding
    // reallocating a `BlockSegmentPostings`.
    //
    // # Warning
    //
    // This does not reset the positions list.
    pub(crate) fn reset(&mut self, doc_freq: usize, postings_data: &'a [u8]) {
        let num_binpacked_blocks: usize = (doc_freq as usize) / NUM_DOCS_PER_BLOCK;
        let num_vint_docs = (doc_freq as usize) - NUM_DOCS_PER_BLOCK * num_binpacked_blocks;
        self.num_binpacked_blocks = num_binpacked_blocks;
        self.num_vint_docs = num_vint_docs;
        self.remaining_data = postings_data;
        self.doc_offset = 0;
        self.doc_freq = doc_freq;
    }

    /// Returns the document frequency associated to this block postings.
    ///
    /// This `doc_freq` is simply the sum of the length of all of the blocks
    /// length, and it does not take in account deleted documents.
    pub fn doc_freq(&self) -> usize {
        self.doc_freq
    }

    /// Returns the array of docs in the current block.
    ///
    /// Before the first call to `.advance()`, the block
    /// returned by `.docs()` is empty.
    #[inline]
    pub fn docs(&self) -> &[DocId] {
        self.block_decoder.output_array()
    }

    /// Returns the length of the current block.
    ///
    /// All blocks have a length of `NUM_DOCS_PER_BLOCK`,
    /// except the last block that may have a length
    /// of any number between 1 and `NUM_DOCS_PER_BLOCK - 1`
    #[inline]
    fn block_len(&self) -> usize {
        self.block_decoder.output_len
    }


    /// Returns a reference to the frequency handler.
    pub fn freq_handler(&self) -> &FreqHandler {
        &self.freq_handler
    }

    /// Advance to the next block.
    ///
    /// Returns false iff there was no remaining blocks.
    pub fn advance(&mut self) -> bool {
        if self.num_binpacked_blocks > 0 {
            self.remaining_data =
                self.block_decoder
                    .uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_handler.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
            self.num_binpacked_blocks -= 1;
            true
        } else if self.num_vint_docs > 0 {
            self.remaining_data =
                self.block_decoder
                    .uncompress_vint_sorted(self.remaining_data,
                                            self.doc_offset,
                                            self.num_vint_docs);
            self.freq_handler
                .read_freq_vint(self.remaining_data, self.num_vint_docs);
            self.num_vint_docs = 0;
            true
        } else {
            false
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> BlockSegmentPostings<'static> {
        BlockSegmentPostings {
            num_binpacked_blocks: 0,
            num_vint_docs: 0,
            block_decoder: BlockDecoder::new(),
            freq_handler: FreqHandler::new_without_freq(),
            remaining_data: &EMPTY_DATA,
            doc_offset: 0,
            doc_freq: 0,
        }
    }
}

impl<'a, 'b> Streamer<'b> for BlockSegmentPostings<'a> {
    type Item = &'b [DocId];

    fn next(&'b mut self) -> Option<&'b [DocId]> {
        if self.advance() {
            Some(self.docs())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use DocSet;
    use super::SegmentPostings;
    use schema::SchemaBuilder;
    use core::Index;
    use schema::INT_INDEXED;
    use schema::Term;
    use fst::Streamer;
    use postings::SegmentPostingsOption;
    use common::HasLen;
    use super::BlockSegmentPostings;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert!(!postings.advance());
        assert!(!postings.advance());
        assert_eq!(postings.len(), 0);
    }

    #[test]
    fn test_empty_block_segment_postings() {
        let mut postings = BlockSegmentPostings::empty();
        assert!(!postings.advance());
        assert_eq!(postings.doc_freq(), 0);
    }

    #[test]
    fn test_block_segment_postings() {
        let mut schema_builder = SchemaBuilder::default();
        let int_field = schema_builder.add_u64_field("id", INT_INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        for _ in 0..100_000 {
            let doc = doc!(int_field=>0u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);
        let term = Term::from_field_u64(int_field, 0u64);
        let term_info = segment_reader.get_term_info(&term).unwrap();
        let mut block_segments =
            segment_reader
                .read_block_postings_from_terminfo(&term_info, SegmentPostingsOption::NoFreq);
        let mut offset: u32 = 0u32;
        // checking that the block before calling advance is empty
        assert!(block_segments.docs().is_empty());
        // checking that the `doc_freq` is correct
        assert_eq!(block_segments.doc_freq(), 100_000);
        while let Some(block) = block_segments.next() {
            for (i, doc) in block.iter().cloned().enumerate() {
                assert_eq!(offset + (i as u32), doc);
            }
            offset += block.len() as u32;
        }
    }


    #[test]
    fn test_reset_block_segment_postings() {
        let mut schema_builder = SchemaBuilder::default();
        let int_field = schema_builder.add_u64_field("id", INT_INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        // create two postings list, one containg even number,
        // the other containing odd numbers.
        for i in 0..6 {
            let doc = doc!(int_field=> (i % 2) as u64);
            index_writer.add_document(doc);
        }
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);

        let mut block_segments;
        {
            let term = Term::from_field_u64(int_field, 0u64);
            let term_info = segment_reader.get_term_info(&term).unwrap();
            block_segments =
                segment_reader
                    .read_block_postings_from_terminfo(&term_info, SegmentPostingsOption::NoFreq);
        }
        assert!(block_segments.advance());
        assert!(block_segments.docs() == &[0, 2, 4]);
        {
            let term = Term::from_field_u64(int_field, 1u64);
            let term_info = segment_reader.get_term_info(&term).unwrap();
            segment_reader.reset_block_postings_from_terminfo(&term_info, &mut block_segments);
        }
        assert!(block_segments.advance());
        assert!(block_segments.docs() == &[1, 3, 5]);
    }
}
