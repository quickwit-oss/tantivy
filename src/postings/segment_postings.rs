use postings::Postings;
use postings::FreqHandler;
use compression::{NUM_DOCS_PER_BLOCK, SIMDBlockDecoder};
use DocId;
use std::cmp::Ordering;
use postings::SkipResult;
use std::num::Wrapping;


// No Term Frequency, no postings.
pub struct SegmentPostings<'a> {
    doc_freq: usize,
    doc_offset: u32,
    block_decoder: SIMDBlockDecoder,
    freq_reader: FreqHandler,
    remaining_data: &'a [u8],
    cur: Wrapping<usize>,
}

const EMPTY_ARRAY: [u8; 0] = [];

impl<'a> SegmentPostings<'a> {

    pub fn empty() -> SegmentPostings<'a> {
        SegmentPostings {
            doc_freq: 0,
            doc_offset: 0,
            block_decoder: SIMDBlockDecoder::new(),
            freq_reader: FreqHandler::NoFreq,
            remaining_data: &EMPTY_ARRAY,
            cur: Wrapping(usize::max_value()),
        }
    }

    pub fn load_next_block(&mut self,) {
        let num_remaining_docs = self.doc_freq - self.cur.0;
        if num_remaining_docs >= NUM_DOCS_PER_BLOCK {
            self.remaining_data = self.block_decoder.uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_reader.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output()[NUM_DOCS_PER_BLOCK - 1];
        }
        else {
            self.remaining_data = self.block_decoder.uncompress_vint_sorted(self.remaining_data, self.doc_offset, num_remaining_docs);
            self.freq_reader.read_freq_vint(self.remaining_data, num_remaining_docs);
        }
    }

    pub fn from_data(doc_freq: u32, data: &'a [u8]) -> SegmentPostings<'a> {
        SegmentPostings {
            doc_freq: doc_freq as usize,
            doc_offset: 0,
            block_decoder: SIMDBlockDecoder::new(),
            freq_reader: FreqHandler::NoFreq,
            remaining_data: data,
            cur: Wrapping(usize::max_value()),
        }
    }
}

impl<'a> Postings for SegmentPostings<'a> {

    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    fn next(&mut self,) -> bool {
        self.cur += Wrapping(1);
        if self.cur.0 >= self.doc_freq {
            return false;
        }
        if self.cur.0 % NUM_DOCS_PER_BLOCK == 0 {
            self.load_next_block();
        }
        return true;
    }

    fn doc(&self,) -> DocId {
        self.block_decoder.output()[self.cur.0 % NUM_DOCS_PER_BLOCK]
    }

    // after skipping position
    // the iterator in such a way that doc() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        loop {
            match self.doc().cmp(&target) {
                Ordering::Equal => {
                    return SkipResult::Reached;
                }
                Ordering::Greater => {
                    return SkipResult::OverStep;
                }
                Ordering::Less => {}
            }
            if !self.next() {
                return SkipResult::End;
            }
        }
    }

    fn doc_freq(&self,) -> usize {
        self.doc_freq
    }
}
