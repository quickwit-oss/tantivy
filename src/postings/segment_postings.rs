use crate::common::HasLen;

use crate::docset::DocSet;
use crate::positions::PositionReader;

use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::postings::serializer::PostingsSerializer;
use crate::postings::BlockSearcher;

use crate::postings::Postings;

use crate::schema::IndexRecordOption;
use crate::DocId;

use crate::directory::ReadOnlySource;
use crate::postings::BlockSegmentPostings;

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
pub struct SegmentPostings {
    pub(crate) block_cursor: BlockSegmentPostings,
    cur: usize,
    position_reader: Option<PositionReader>,
    block_searcher: BlockSearcher,
}

impl SegmentPostings {
    /// Returns an empty segment postings object
    pub fn empty() -> Self {
        SegmentPostings {
            block_cursor: BlockSegmentPostings::empty(),
            cur: 0,
            position_reader: None,
            block_searcher: BlockSearcher::default(),
        }
    }

    pub fn doc_freq(&self) -> u32 {
        self.block_cursor.doc_freq()
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
        position_reader: Option<PositionReader>,
    ) -> SegmentPostings {
        SegmentPostings {
            block_cursor: segment_block_postings,
            cur: 0, // cursor within the block
            position_reader,
            block_searcher: BlockSearcher::default(),
        }
    }
}

impl DocSet for SegmentPostings {
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    #[inline]
    fn advance(&mut self) -> DocId {
        if self.cur == COMPRESSION_BLOCK_SIZE - 1 {
            self.cur = 0;
            self.block_cursor.advance();
        } else {
            self.cur += 1;
        }
        self.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if self.doc() == target {
            return target;
        }
        self.block_cursor.seek(target);

        // At this point we are on the block, that might contain our document.
        let output = self.block_cursor.docs_aligned();

        self.cur = self.block_searcher.search_in_block(&output, target);

        // The last block is not full and padded with the value TERMINATED,
        // so that we are guaranteed to have at least doc in the block (a real one or the padding)
        // that is greater or equal to the target.
        debug_assert!(self.cur < COMPRESSION_BLOCK_SIZE);

        // `doc` is now the first element >= `target`

        // If all docs are smaller than target the current block should be incomplemented and padded
        // with the value `TERMINATED`.
        //
        // After the search, the cursor should point to the first value of TERMINATED.
        let doc = output.0[self.cur];
        debug_assert!(doc >= target);
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

impl HasLen for SegmentPostings {
    fn len(&self) -> usize {
        self.block_cursor.doc_freq() as usize
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
        if let Some(position_reader) = self.position_reader.as_mut() {
            let read_offset = self.block_cursor.position_offset()
                + (self.block_cursor.freqs()[..self.cur]
                    .iter()
                    .cloned()
                    .sum::<u32>() as u64);
            output.resize(term_freq, 0u32);
            position_reader.read(read_offset, &mut output[..]);
            let mut cum = offset;
            for output_mut in output.iter_mut() {
                cum += *output_mut;
                *output_mut = cum;
            }
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
