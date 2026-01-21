use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::query::Bm25Weight;
use crate::Score;

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
    /// The number of times the term appears in the document.
    fn term_freq(&self) -> u32;

    /// Returns, if available, the number of documents containing the term in the segment.
    fn doc_freq(&self) -> u32;

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

    fn has_freq(&self) -> bool;
}

impl Postings for Box<dyn Postings> {
    fn term_freq(&self) -> u32 {
        (**self).term_freq()
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        (**self).append_positions_with_offset(offset, output);
    }

    fn has_freq(&self) -> bool {
        (**self).has_freq()
    }

    fn doc_freq(&self) -> u32 {
        (**self).doc_freq()
    }
}

impl Postings for Box<dyn PostingsWithBlockMax> {
    fn term_freq(&self) -> u32 {
        (**self).term_freq()
    }

    fn append_positions_with_offset(&mut self, offset: u32, output: &mut Vec<u32>) {
        (**self).append_positions_with_offset(offset, output);
    }

    fn has_freq(&self) -> bool {
        (**self).has_freq()
    }

    fn doc_freq(&self) -> u32 {
        (**self).doc_freq()
    }
}

pub trait PostingsWithBlockMax: Postings {
    fn seek_block(
        &mut self,
        target_doc: crate::DocId,
        fieldnorm_reader: &FieldNormReader,
        similarity_weight: &Bm25Weight,
    ) -> Score;

    fn last_doc_in_block(&self) -> crate::DocId;
}

impl PostingsWithBlockMax for Box<dyn PostingsWithBlockMax> {
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
}
