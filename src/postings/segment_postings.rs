use crate::common::HasLen;
use crate::docset::DocSet;
use crate::fastfield::DeleteBitSet;
use crate::positions::PositionReader;
use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::postings::BlockSearcher;
use crate::postings::BlockSegmentPostings;
use crate::postings::Postings;
use crate::{DocId, TERMINATED};

/// `SegmentPostings` represents the inverted list or postings associated to
/// a term in a `Segment`.
///
/// As we iterate through the `SegmentPostings`, the frequencies are optionally decoded.
/// Positions on the other hand, are optionally entirely decoded upfront.
#[derive(Clone)]
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

    /// Compute the number of non-deleted documents.
    ///
    /// This method will clone and scan through the posting lists.
    /// (this is a rather expensive operation).
    pub fn doc_freq_given_deletes(&self, delete_bitset: &DeleteBitSet) -> u32 {
        let mut docset = self.clone();
        let mut doc_freq = 0;
        loop {
            let doc = docset.doc();
            if doc == TERMINATED {
                return doc_freq;
            }
            if delete_bitset.is_alive(doc) {
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

    /// Creates a segment postings object with the given documents
    /// and no frequency encoded.
    ///
    /// This method is mostly useful for unit tests.
    ///
    /// It serializes the doc ids using tantivy's codec
    /// and returns a `SegmentPostings` object that embeds a
    /// buffer with the serialized data.
    #[cfg(test)]
    pub fn create_from_docs(docs: &[u32]) -> SegmentPostings {
        use crate::directory::FileSlice;
        use crate::postings::serializer::PostingsSerializer;
        use crate::schema::IndexRecordOption;
        let mut buffer = Vec::new();
        {
            let mut postings_serializer =
                PostingsSerializer::new(&mut buffer, 0.0, IndexRecordOption::Basic, None);
            postings_serializer.new_term(docs.len() as u32);
            for &doc in docs {
                postings_serializer.write_doc(doc, 1u32);
            }
            postings_serializer
                .close_term(docs.len() as u32)
                .expect("In memory Serialization should never fail.");
        }
        let block_segment_postings = BlockSegmentPostings::open(
            docs.len() as u32,
            FileSlice::from(buffer),
            IndexRecordOption::Basic,
            IndexRecordOption::Basic,
        )
        .unwrap();
        SegmentPostings::from_block_postings(block_segment_postings, None)
    }

    /// Helper functions to create `SegmentPostings` for tests.
    #[cfg(test)]
    pub fn create_from_docs_and_tfs(
        doc_and_tfs: &[(u32, u32)],
        fieldnorms: Option<&[u32]>,
    ) -> SegmentPostings {
        use crate::directory::FileSlice;
        use crate::fieldnorm::FieldNormReader;
        use crate::postings::serializer::PostingsSerializer;
        use crate::schema::IndexRecordOption;
        use crate::Score;
        let mut buffer: Vec<u8> = Vec::new();
        let fieldnorm_reader = fieldnorms.map(FieldNormReader::for_test);
        let average_field_norm = fieldnorms
            .map(|fieldnorms| {
                if fieldnorms.len() == 0 {
                    return 0.0;
                }
                let total_num_tokens: u64 = fieldnorms
                    .iter()
                    .map(|&fieldnorm| fieldnorm as u64)
                    .sum::<u64>();
                total_num_tokens as Score / fieldnorms.len() as Score
            })
            .unwrap_or(0.0);
        let mut postings_serializer = PostingsSerializer::new(
            &mut buffer,
            average_field_norm,
            IndexRecordOption::WithFreqs,
            fieldnorm_reader,
        );
        postings_serializer.new_term(doc_and_tfs.len() as u32);
        for &(doc, tf) in doc_and_tfs {
            postings_serializer.write_doc(doc, tf);
        }
        postings_serializer
            .close_term(doc_and_tfs.len() as u32)
            .unwrap();
        let block_segment_postings = BlockSegmentPostings::open(
            doc_and_tfs.len() as u32,
            FileSlice::from(buffer),
            IndexRecordOption::WithFreqs,
            IndexRecordOption::WithFreqs,
        )
        .unwrap();
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
        debug_assert_eq!(doc, self.doc());
        doc
    }

    /// Return the current document's `DocId`.
    #[inline(always)]
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
    use crate::fastfield::DeleteBitSet;
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

    #[test]
    fn test_doc_freq() {
        let docs = SegmentPostings::create_from_docs(&[0, 2, 10]);
        assert_eq!(docs.doc_freq(), 3);
        let delete_bitset = DeleteBitSet::for_test(&[2], 12);
        assert_eq!(docs.doc_freq_given_deletes(&delete_bitset), 2);
        let all_deleted = DeleteBitSet::for_test(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 12);
        assert_eq!(docs.doc_freq_given_deletes(&all_deleted), 0);
    }
}
