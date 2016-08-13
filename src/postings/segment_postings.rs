use compression::{NUM_DOCS_PER_BLOCK, SIMDBlockDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen};
use std::num::Wrapping;




// No Term Frequency, no postings.
pub struct SegmentPostings<'a> {
    len: usize,
    doc_offset: u32,
    block_decoder: SIMDBlockDecoder,
    freq_handler: FreqHandler,
    remaining_data: &'a [u8],
    cur: Wrapping<usize>,
}

const EMPTY_ARRAY: [u8; 0] = [];

impl<'a> SegmentPostings<'a> {

    pub fn empty() -> SegmentPostings<'a> {
        SegmentPostings {
            len: 0,
            doc_offset: 0,
            block_decoder: SIMDBlockDecoder::new(),
            freq_handler: FreqHandler::new(),
            remaining_data: &EMPTY_ARRAY,
            cur: Wrapping(usize::max_value()),
        }
    }
    
    pub fn load_next_block(&mut self,) {
        let num_remaining_docs = self.len - self.cur.0;
        if num_remaining_docs >= NUM_DOCS_PER_BLOCK {
            self.remaining_data = self.block_decoder.uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_handler.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
        }
        else {
            self.remaining_data = self.block_decoder.uncompress_vint_sorted(self.remaining_data, self.doc_offset, num_remaining_docs);
            self.freq_handler.read_freq_vint(self.remaining_data, num_remaining_docs);
        }
    }

    pub fn from_data(len: u32, data: &'a [u8], freq_handler: FreqHandler) -> SegmentPostings<'a> {
        SegmentPostings {
            len: len as usize,
            doc_offset: 0,
            block_decoder: SIMDBlockDecoder::new(),
            freq_handler: freq_handler,
            remaining_data: data,
            cur: Wrapping(usize::max_value()),
        }
    }

    #[inline(always)]
    fn index_within_block(&self,) -> usize {
        self.cur.0 % NUM_DOCS_PER_BLOCK
    }

}


impl<'a> DocSet for SegmentPostings<'a> {

    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline(always)]
    fn advance(&mut self,) -> bool {
        self.cur += Wrapping(1);
        if self.cur.0 >= self.len {
            return false;
        }
        if self.index_within_block() == 0 {
            self.load_next_block();
        }
        return true;
    }

    #[inline(always)]
    fn doc(&self,) -> DocId {
        self.block_decoder.output(self.index_within_block())
    }

}

impl<'a> HasLen for SegmentPostings<'a> {
    fn len(&self,) -> usize {
        self.len
    }
}

impl<'a> Postings for SegmentPostings<'a> {
    fn term_freq(&self,) -> u32 {
        self.freq_handler.freq(self.index_within_block())
    }
    
    fn positions(&self) -> &[u32] {
        self.freq_handler.positions(self.index_within_block())
    }
}

