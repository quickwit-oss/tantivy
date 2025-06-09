use common::HasLen;

use crate::docset::DocSet;
use crate::fastfield::AliveBitSet;
use crate::positions::borrowed_position_reader::BorrowedPositionReader;
use crate::postings::borrowed_block_segment_postings::BorrowedBlockSegmentPostings;
use crate::postings::branchless_binary_search;
use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::{DocId, TERMINATED};

/// `SegmentPostings` represents the inverted list or postings associated with
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
#[derive(Clone)]
pub struct BorrowedSegmentPostings<'a> {
    pub(crate) block_cursor: BorrowedBlockSegmentPostings<'a>,
    cur: usize,
    position_reader: Option<BorrowedPositionReader<'a>>,
}

impl BorrowedSegmentPostings<'_> {
    /// Compute the number of non-deleted documents.
    ///
    /// This method will clone and scan through the posting lists.
    /// (this is a rather expensive operation).
    pub fn doc_freq_given_deletes(&self, alive_bitset: &AliveBitSet) -> u32 {
        let mut docset = self.clone();
        let mut doc_freq = 0;
        loop {
            let doc = docset.doc();
            if doc == TERMINATED {
                return doc_freq;
            }
            if alive_bitset.is_alive(doc) {
                doc_freq += 1u32;
            }
            docset.advance();
        }
    }

    /// Returns the overall number of documents in the block postings.
    /// It does not take in account whether documents are deleted or not.
    pub fn doc_freq(&self) -> u32 {
        self.block_cursor.doc_freq()
    }

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding frequencies and/or positions
    pub(crate) fn from_block_postings<'a>(
        segment_block_postings: BorrowedBlockSegmentPostings<'a>,
        position_reader: Option<BorrowedPositionReader<'a>>,
    ) -> BorrowedSegmentPostings<'a> {
        BorrowedSegmentPostings {
            block_cursor: segment_block_postings,
            cur: 0, // cursor within the block
            position_reader,
        }
    }

    pub fn term_freq(&self) -> u32 {
        debug_assert!(
            // Here we do not use the len of `freqs()`
            // because it is actually ok to request for the freq of doc
            // even if no frequency were encoded for the field.
            //
            // In that case we hit the block just as if the frequency had been
            // decoded. The block is simply prefilled by the value 1.
            self.cur < COMPRESSION_BLOCK_SIZE,
            "Have you forgotten to call `.advance()` at least once before calling `.term_freq()`."
        );
        self.block_cursor.freq(self.cur)
    }

    /// Returns the positions offsetted with a given value.
    /// It is not necessary to clear the `output` before calling this method.
    /// The output vector will be resized to the `term_freq`.
    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        output.clear();
        self.append_positions_with_offset(offset, output);
    }

    /// Returns the positions offsetted with a given value.
    /// Data will be appended to the output.
    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let term_freq = self.term_freq();
        let prev_len = output.len();
        if let Some(position_reader) = self.position_reader.as_mut() {
            debug_assert!(
                !self.block_cursor.freqs().is_empty(),
                "No positions available"
            );
            let read_offset = self.block_cursor.position_offset()
                + (self.block_cursor.freqs()[..self.cur]
                    .iter()
                    .cloned()
                    .sum::<u32>() as u64);
            // TODO: instead of zeroing the output, we could use MaybeUninit or similar.
            output.resize(prev_len + term_freq as usize, 0u32);
            position_reader.read(read_offset, &mut output[prev_len..]);
            let mut cum = offset;
            for output_mut in output[prev_len..].iter_mut() {
                cum += *output_mut;
                *output_mut = cum;
            }
        }
    }

    /// Returns the positions of the term in the given document.
    /// The output vector will be resized to the `term_freq`.
    pub fn positions(&mut self, output: &mut Vec<u32>) {
        self.positions_with_offset(0u32, output);
    }
}

impl DocSet for BorrowedSegmentPostings<'_> {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> DocId {
        debug_assert!(self.block_cursor.block_is_loaded());
        if self.cur == COMPRESSION_BLOCK_SIZE - 1 {
            self.cur = 0;
            self.block_cursor.advance();
        } else {
            self.cur += 1;
        }
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(self.doc() <= target);
        if self.doc() >= target {
            return self.doc();
        }

        self.block_cursor.seek(target);

        // At this point we are on the block, that might contain our document.
        let output = self.block_cursor.full_block();
        self.cur = branchless_binary_search(output, target);

        // The last block is not full and padded with the value TERMINATED,
        // so that we are guaranteed to have at least doc in the block (a real one or the padding)
        // that is greater or equal to the target.
        debug_assert!(self.cur < COMPRESSION_BLOCK_SIZE);

        // `doc` is now the first element >= `target`

        // If all docs are smaller than target the current block should be incomplemented and padded
        // with the value `TERMINATED`.
        //
        // After the search, the cursor should point to the first value of TERMINATED.
        let doc = output[self.cur];
        debug_assert!(doc >= target);
        debug_assert_eq!(doc, self.doc());
        doc
    }

    /// Return the current document's `DocId`.
    #[inline]
    fn doc(&self) -> DocId {
        self.block_cursor.doc(self.cur)
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }
}

impl HasLen for BorrowedSegmentPostings<'_> {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq() as usize
    }
}
