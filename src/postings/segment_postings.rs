use compression::{NUM_DOCS_PER_BLOCK, SIMDBlockDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen};
use std::num::Wrapping;


const EMPTY_DATA: [u8; 0] = [0u8; 0];

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings<'a> {
    len: usize,
    doc_offset: u32,
    block_decoder: SIMDBlockDecoder,
    freq_handler: FreqHandler,
    remaining_data: &'a [u8],
    cur: Wrapping<usize>,
}

impl<'a> SegmentPostings<'a> {
    fn load_next_block(&mut self) {
        let num_remaining_docs = self.len - self.cur.0;
        if num_remaining_docs >= NUM_DOCS_PER_BLOCK {
            self.remaining_data = self.block_decoder
                .uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_handler.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
        } else {
            self.remaining_data = self.block_decoder
                .uncompress_vint_sorted(self.remaining_data, self.doc_offset, num_remaining_docs);
            self.freq_handler.read_freq_vint(self.remaining_data, num_remaining_docs);
        }
    }

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
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

    /// Returns an empty segment postings object
    pub fn empty() -> SegmentPostings<'static> {
        SegmentPostings {
            len: 0,
            doc_offset: 0,
            block_decoder: SIMDBlockDecoder::new(),
            freq_handler: FreqHandler::new_without_freq(),
            remaining_data: &EMPTY_DATA,
            cur: Wrapping(usize::max_value()),
        }
    }

    /// Index within a block is used as an address when
    /// interacting with the `FreqHandler`
    fn index_within_block(&self) -> usize {
        self.cur.0 % NUM_DOCS_PER_BLOCK
    }
}


impl<'a> DocSet for SegmentPostings<'a> {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> bool {
        self.cur += Wrapping(1);
        if self.cur.0 >= self.len {
            return false;
        }
        if self.index_within_block() == 0 {
            self.load_next_block();
        }
        true
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.block_decoder.output(self.index_within_block())
    }
}

impl<'a> HasLen for SegmentPostings<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> Postings for SegmentPostings<'a> {
    fn term_freq(&self) -> u32 {
        self.freq_handler.freq(self.index_within_block())
    }

    fn positions(&self) -> &[u32] {
        self.freq_handler.positions(self.index_within_block())
    }
}
