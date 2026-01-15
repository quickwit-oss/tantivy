use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::postings::FreqReadingOption;
use crate::query::{Bm25Weight, Scorer};
use crate::{DocId, Score};

/// Postings (also called inverted list)
///
/// For a given term, it is the list of doc ids of the doc
/// containing the term. Optionally, for each document,
/// it may also give access to the term frequency
/// as well as the list of term positions.
///
/// Its main implementation is `SegmentPostings`,
/// but other implementations mocking `SegmentPostings` exist,
/// for merging segments or for testing.
pub trait Postings: DocSet + 'static {
    fn new_term_scorer(self: Box<Self>, fieldnorm_reader: &FieldNormReader, similarity_weight: &Bm25Weight) -> Box<dyn Scorer>;

    /// The number of times the term appears in the document.
    fn term_freq(&self) -> u32;

    /// Returns the positions offsetted with a given value.
    /// It is not necessary to clear the `output` before calling this method.
    /// The output vector will be resized to the `term_freq`.
    fn positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        output.clear();
        self.append_positions_with_offset(offset, output);
    }

    /// Returns the positions offsetted with a given value.
    /// Data will be appended to the output.
    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>);

    /// Returns the positions of the term in the given document.
    /// The output vector will be resized to the `term_freq`.
    fn positions(&mut self, output: &mut Vec<u32>) {
        self.positions_with_offset(0u32, output);
    }

    // supports Block-Wand
    fn supports_block_max(&self) -> bool {
        false
    }

    // TODO document
    // Only allowed for block max.
    fn seek_block(
        &mut self,
        target_doc: crate::DocId,
        fieldnorm_reader: &FieldNormReader,
        similarity_weight: &Bm25Weight,
    ) -> Score {
        unimplemented!()
    }

    // TODO
    // Only allowed for block max.
    fn last_doc_in_block(&self) -> crate::DocId {
        unimplemented!()
    }

    fn freq_reading_option(&self) -> FreqReadingOption;
}

impl Postings for Box<dyn Postings> {
    fn term_freq(&self) -> u32 {
        (**self).term_freq()
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        (**self).append_positions_with_offset(offset, output);
    }

    fn supports_block_max(&self) -> bool {
        (**self).supports_block_max()
    }

    fn seek_block(
        &mut self,
        target_doc: crate::DocId,
        fieldnorm_reader: &FieldNormReader,
        similarity_weight: &Bm25Weight,
    ) -> Score {
        (**self).seek_block(target_doc, fieldnorm_reader, similarity_weight)
    }

    fn last_doc_in_block(&self) -> crate::DocId {
        (**self).last_doc_in_block()
    }

    fn freq_reading_option(&self) -> FreqReadingOption {
        (**self).freq_reading_option()
    }
}
