use compression::{NUM_DOCS_PER_BLOCK, BlockDecoder, VIntDecoder};
use DocId;
use postings::{Postings, FreqHandler, DocSet, HasLen, SkipResult};
use std::cmp;
use std::num::Wrapping;
use fastfield::DeleteBitSet;


const EMPTY_DATA: [u8; 0] = [0u8; 0];

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings<'a> {
    len: usize,
    // Removing this makes the code slower
    // See https://github.com/tantivy-search/tantivy/issues/89
    block_len: usize,
    doc_offset: u32,
    block_decoder: BlockDecoder,
    freq_handler: FreqHandler,
    remaining_data: &'a [u8],
    cur: Wrapping<usize>,
    delete_bitset: DeleteBitSet,
}

impl<'a> SegmentPostings<'a> {
    fn load_next_block(&mut self) {
        let num_remaining_docs = self.len - self.cur.0;
        if num_remaining_docs >= NUM_DOCS_PER_BLOCK {
            self.remaining_data =
                self.block_decoder
                    .uncompress_block_sorted(self.remaining_data, self.doc_offset);
            self.remaining_data = self.freq_handler.read_freq_block(self.remaining_data);
            self.doc_offset = self.block_decoder.output(NUM_DOCS_PER_BLOCK - 1);
            self.block_len = NUM_DOCS_PER_BLOCK;
        } else {
            self.remaining_data =
                self.block_decoder
                    .uncompress_vint_sorted(self.remaining_data,
                                            self.doc_offset,
                                            num_remaining_docs);
            self.freq_handler
                .read_freq_vint(self.remaining_data, num_remaining_docs);
            self.block_len = num_remaining_docs;
        }
    }

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub fn from_data(len: u32,
                     data: &'a [u8],
                     delete_bitset: &'a DeleteBitSet,
                     freq_handler: FreqHandler)
                     -> SegmentPostings<'a> {
        SegmentPostings {
            len: len as usize,
            block_len: len as usize,
            doc_offset: 0,
            block_decoder: BlockDecoder::new(),
            freq_handler: freq_handler,
            remaining_data: data,
            cur: Wrapping(usize::max_value()),
            delete_bitset: delete_bitset.clone(),
        }
    }

    /// Returns an empty segment postings object
    pub fn empty() -> SegmentPostings<'static> {
        SegmentPostings {
            len: 0,
            block_len: 0,
            doc_offset: 0,
            block_decoder: BlockDecoder::new(),
            freq_handler: FreqHandler::new_without_freq(),
            remaining_data: &EMPTY_DATA,
            delete_bitset: DeleteBitSet::empty(),
            cur: Wrapping(usize::max_value()),
        }
    }

    /// Index within a block is used as an address when
    /// interacting with the `FreqHandler`
    fn index_within_block(&self) -> usize {
        self.cur.0 % NUM_DOCS_PER_BLOCK
    }

    /// Sets the current position to a location relative
    /// to the current block
    #[inline]
    fn set_within_block(&mut self, inner_pos: usize) {
        self.cur = Wrapping(self.cur.0 & !(NUM_DOCS_PER_BLOCK - 1)) + Wrapping(inner_pos)
    }
}


impl<'a> DocSet for SegmentPostings<'a> {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> bool {
        loop {
            self.cur += Wrapping(1);
            if self.cur.0 >= self.len {
                return false;
            }
            if self.index_within_block() == 0 {
                self.load_next_block();
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

        let mut pos = self.index_within_block();
        // skip blocks until one that might contain the target
        loop {
            // check if we need to go to the next block
            if target > self.block_decoder.output(self.block_len - 1) {
                self.cur += Wrapping(self.block_len - pos);
                self.load_next_block();
                pos = 0;

                // there was no more data
                if self.cur.0 == self.len {
                    return SkipResult::End;
                }
            } else if target < self.block_decoder.output(pos) {
                // We've overpassed the target after the first `advance` call
                // or we're at the beginning of a block.
                // Either way, we're on the first `DocId` greater than `target`
                return SkipResult::OverStep;
            } else {
                break;
            }
        }

        debug_assert!(target >= self.block_decoder.output(pos));
        debug_assert!(target <= self.block_decoder.output(self.block_len - 1));

        // we're in the right block now, start with an exponential search
        let mut start = pos;
        let mut end = self.block_len;
        let mut count = 1;
        loop {
            let new = start + count;
            if new < end && self.block_decoder.output(new) < target {
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
            let doc = self.block_decoder.output(mid);
            if doc < target {
                start = mid + 1;
                count -= step + 1;
            } else {
                count = step;
            }
        }

        // `doc` is now >= `target`
        let doc = self.block_decoder.output(start);
        self.set_within_block(start);

        if !self.delete_bitset.is_deleted(doc) {
            if doc == target {
                return SkipResult::Reached;
            } else {
                return SkipResult::OverStep;
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
