use crate::common::HasLen;

use crate::docset::{DocSet, TERMINATED};
use crate::positions::PositionReader;

use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::postings::serializer::PostingsSerializer;
use crate::postings::BlockSearcher;

use crate::postings::Postings;

use crate::schema::IndexRecordOption;
use crate::DocId;

use crate::postings::{BlockSegmentPostings, BlockSegmentPostingsSkipResult};
use crate::directory::ReadOnlySource;

struct PositionComputer {
    // store the amount of position int
    // before reading positions.
    //
    // if none, position are already loaded in
    // the positions vec.
    position_to_skip: usize,
    position_reader: PositionReader,
}

impl PositionComputer {
    pub fn new(position_reader: PositionReader) -> PositionComputer {
        PositionComputer {
            position_to_skip: 0,
            position_reader,
        }
    }

    pub fn add_skip(&mut self, num_skip: usize) {
        self.position_to_skip += num_skip;
    }

    // Positions can only be read once.
    pub fn positions_with_offset(&mut self, offset: u32, output: &mut [u32]) {
        self.position_reader.skip(self.position_to_skip);
        self.position_to_skip = 0;
        self.position_reader.read(output);
        let mut cum = offset;
        for output_mut in output.iter_mut() {
            cum += *output_mut;
            *output_mut = cum;
        }
    }
}

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings {
    block_cursor: BlockSegmentPostings,
    cur: usize,
    position_computer: Option<PositionComputer>,
    block_searcher: BlockSearcher,
}

impl SegmentPostings {
    /// Returns an empty segment postings object
    pub fn empty() -> Self {
        SegmentPostings {
            block_cursor: BlockSegmentPostings::empty(),
            cur: 0,
            position_computer: None,
            block_searcher: BlockSearcher::default(),
        }
    }

    /// Creates a segment postings object with the given documents
    /// and no frequency encoded.
    ///
    /// This method is mostly useful for unit tests.
    ///
    /// It serializes the doc ids using tantivy's codec
    /// and returns a `SegmentPostings` object that embeds a
    /// buffer with the serialized data.
    pub fn create_from_docs(docs: &[u32]) -> SegmentPostings {
        let mut buffer = Vec::new();
        {
            let mut postings_serializer = PostingsSerializer::new(&mut buffer, false, false);
            for &doc in docs {
                postings_serializer.write_doc(doc, 1u32);
            }
            postings_serializer
                .close_term(docs.len() as u32)
                .expect("In memory Serialization should never fail.");
        }
        let block_segment_postings = BlockSegmentPostings::from_data(
            docs.len() as u32,
            ReadOnlySource::from(buffer),
            IndexRecordOption::Basic,
            IndexRecordOption::Basic,
        );
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }

    /// Reads a Segment postings from an &[u8]
    ///
    /// * `len` - number of document in the posting lists.
    /// * `data` - data array. The complete data is not necessarily used.
    /// * `freq_handler` - the freq handler is in charge of decoding
    ///   frequencies and/or positions
    pub(crate) fn from_block_postings(
        segment_block_postings: BlockSegmentPostings,
        positions_stream_opt: Option<PositionReader>,
    ) -> SegmentPostings {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: 0, // cursor within the block
            position_computer: positions_stream_opt.map(PositionComputer::new),
            block_searcher: BlockSearcher::default(),
        }
    }
}

impl DocSet for SegmentPostings {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> DocId {
        if self.position_computer.is_some() && self.cur < COMPRESSION_BLOCK_SIZE {
            let term_freq = self.term_freq() as usize;
            if let Some(position_computer) = self.position_computer.as_mut() {
                position_computer.add_skip(term_freq);
            }
        }
        self.cur += 1;
        if self.cur >= self.block_cursor.block_len() {
            if self.block_cursor.advance() {
                self.cur = 0;
            } else {
                self.cur = COMPRESSION_BLOCK_SIZE - 1;
                return TERMINATED;
            }
        }
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if self.doc() >= target {
            return self.doc();
        }

        // In the following, thanks to the call to advance above,
        // we know that the position is not loaded and we need
        // to skip every doc_freq we cross.

        // skip blocks until one that might contain the target
        // check if we need to go to the next block
        let mut sum_freqs_skipped: u32 = 0;
        if self
            .block_cursor
            .docs()
            .last()
            .map(|&doc| doc < target)
            .unwrap_or(true)
        {
            // We are not in the right block.
            if self.position_computer.is_some() {
                // First compute all of the freqs skipped from the current block.
                sum_freqs_skipped = self.block_cursor.freqs()[self.cur..].iter().sum::<u32>();
                match self.block_cursor.seek(target) {
                    BlockSegmentPostingsSkipResult::Success(block_skip_freqs) => {
                        sum_freqs_skipped += block_skip_freqs;
                    }
                    BlockSegmentPostingsSkipResult::Terminated => {
                        self.block_cursor.doc_decoder.clear();
                        self.cur = 0;
                        return TERMINATED;
                    }
                }
            } else if self.block_cursor.seek(target) == BlockSegmentPostingsSkipResult::Terminated {
                // no positions needed. no need to sum freqs.
                self.block_cursor.doc_decoder.clear();
                self.cur = 0;
                return TERMINATED;
            }
            self.cur = 0;
        }

        // At this point we are on the block, that might contain our document.

        let cur = self.cur;

        let output = self.block_cursor.docs_aligned();
        let new_cur = self.block_searcher.search_in_block(&output, cur, target);
        if let Some(position_computer) = self.position_computer.as_mut() {
            sum_freqs_skipped += self.block_cursor.freqs()[cur..new_cur].iter().sum::<u32>();
            position_computer.add_skip(sum_freqs_skipped as usize);
        }
        self.cur = new_cur;

        // `doc` is now the first element >= `target`
        let doc = output.0[new_cur];
        debug_assert!(doc >= target);
        doc
    }

    /// Return the current document's `DocId`.
    ///
    /// # Panics
    ///
    /// Will panics if called without having called advance before.
    #[inline]
    fn doc(&self) -> DocId {
        self.block_cursor.doc(self.cur)
    }

    fn size_hint(&self) -> u32 {
        self.len() as u32
    }
}

impl HasLen for SegmentPostings {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq()
    }
}

impl Postings for SegmentPostings {
    /// Returns the frequency associated to the current document.
    /// If the schema is set up so that no frequency have been encoded,
    /// this method should always return 1.
    ///
    /// # Panics
    ///
    /// Will panics if called without having called advance before.
    fn term_freq(&self) -> u32 {
        debug_assert!(
            // Here we do not use the len of `freqs()`
            // because it is actually ok to request for the freq of doc
            // even if no frequency were encoded for the field.
            //
            // In that case we hit the block just as if the frequency had been
            // decoded. The block is simply prefilled by the value 1.
            self.cur < COMPRESSION_BLOCK_SIZE,
            "Have you forgotten to call `.advance()` at least once before calling \
             `.term_freq()`."
        );
        self.block_cursor.freq(self.cur)
    }

    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        let term_freq = self.term_freq() as usize;
        if let Some(position_comp) = self.position_computer.as_mut() {
            output.resize(term_freq, 0u32);
            position_comp.positions_with_offset(offset, &mut output[..]);
        } else {
            output.clear();
        }
    }
}

#[cfg(test)]
mod tests {

    use super::SegmentPostings;
    use crate::common::HasLen;

    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::postings::Postings;

    #[test]
    fn test_empty_segment_postings() {
        let mut postings = SegmentPostings::empty();
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
        assert_eq!(postings.len(), 0);
    }

    #[test]
    fn test_empty_postings_doc_returns_terminated() {
        let mut postings = SegmentPostings::empty();
        assert_eq!(postings.doc(), TERMINATED);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_empty_postings_doc_term_freq_returns_0() {
        let postings = SegmentPostings::empty();
        assert_eq!(postings.term_freq(), 1);
    }
}
