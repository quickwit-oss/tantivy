use compression::{NUM_DOCS_PER_BLOCK, BlockDecoder, VIntDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen};
use std::num::Wrapping;


const EMPTY_DATA: [u8; 0] = [0u8; 0];


pub struct BlockSegmentPostings<'a> {
    num_binpacked_blocks: usize,
    num_vint_docs: usize,
    block_decoder: BlockDecoder,
    freq_handler: FreqHandler,
    remaining_data: &'a [u8],
    doc_offset: DocId,
    len: usize,
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

    pub fn docs(&self) -> &[DocId] {
        self.block_decoder.output_array()
    }
    
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


/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings<'a> {
    len: usize,
    // doc_offset: usize,
    cur: Wrapping<usize>,
    block_cursor: BlockSegmentPostings<'a>,
    cur_block_len: usize
}

impl<'a> SegmentPostings<'a> {

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub fn from_block_postings(segment_block_postings: BlockSegmentPostings<'a>) -> SegmentPostings<'a> {
        SegmentPostings {
            len: segment_block_postings.len,
            block_cursor: segment_block_postings,
            cur: Wrapping(usize::max_value()),
            cur_block_len: 0,
        }
    }
    
    /// Returns an empty segment postings object
    pub fn empty() -> SegmentPostings<'static> {
        let empty_block_cursor = BlockSegmentPostings::empty();
        SegmentPostings {
            len: 0,
            block_cursor: empty_block_cursor,
            cur: Wrapping(usize::max_value()),
            cur_block_len: 0,
        }
    }
}


impl<'a> DocSet for SegmentPostings<'a> {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> bool {
        self.cur += Wrapping(1);
        if self.cur.0 == self.cur_block_len {
            self.cur = Wrapping(0);
            if !self.block_cursor.advance() {
                self.cur_block_len = 0;
                self.cur = Wrapping(usize::max_value());
                return false;
            }
            self.cur_block_len = self.block_cursor.docs().len();
        }
        true
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.block_cursor.docs()[self.cur.0]
    }
}

impl<'a> HasLen for SegmentPostings<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> Postings for SegmentPostings<'a> {
    fn term_freq(&self) -> u32 {
        self.block_cursor.freq_handler().freq(self.cur.0)
    }

    fn positions(&self) -> &[u32] {
        self.block_cursor.freq_handler().positions(self.cur.0)
    }
}
