use crate::docset::DocSet;

/// Result of the doc_freq method.
///
/// Postings can inform us that the document frequency is approximate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocFreq {
    /// The document frequency is approximate.
    Approximate(u32),
    /// The document frequency is exact.
    Exact(u32),
}

impl From<DocFreq> for u32 {
    fn from(doc_freq: DocFreq) -> Self {
        match doc_freq {
            DocFreq::Approximate(approximate_doc_freq) => approximate_doc_freq,
            DocFreq::Exact(doc_freq) => doc_freq,
        }
    }
}

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

    /// Returns the number of documents containing the term in the segment.
    fn doc_freq(&self) -> DocFreq;

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

    /// Returns true if the term_frequency is available.
    ///
    /// This is a tricky question, because on JSON fields, it is possible
    /// for a text term to have term freq, whereas a number term in the field has none.
    ///
    /// This function returns whether the actual term has term frequencies or not.
    /// In this above JSON field example, `has_freq` should return true for the
    /// earlier and false for the latter.
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

    fn doc_freq(&self) -> DocFreq {
        (**self).doc_freq()
    }
}
