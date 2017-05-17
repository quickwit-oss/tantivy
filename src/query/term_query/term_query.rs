use Term;
use Result;
use super::term_weight::TermWeight;
use query::Query;
use query::Weight;
use postings::SegmentPostingsOption;
use Searcher;
use std::any::Any;

/// A Term query matches all of the documents
/// containing a specific term.
///
/// The score associated is defined as
/// `idf` *  sqrt(`term_freq` / `field norm`)
/// in which :
/// * `idf`        - inverse document frequency.
/// * `term_freq`  - number of occurrences of the term in the field
/// * `field norm` - number of tokens in the field.
#[derive(Debug)]
pub struct TermQuery {
    term: Term,
    segment_postings_options: SegmentPostingsOption,
}

impl TermQuery {
    /// Creates a new term query.
    pub fn new(term: Term, segment_postings_options: SegmentPostingsOption) -> TermQuery {
        TermQuery {
            term: term,
            segment_postings_options: segment_postings_options,
        }
    }


    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(&self, searcher: &Searcher) -> TermWeight {
        TermWeight {
            num_docs: searcher.num_docs(),
            doc_freq: searcher.doc_freq(&self.term),
            term: self.term.clone(),
            segment_postings_options: self.segment_postings_options,
        }
    }
}

impl Query for TermQuery {
    fn as_any(&self) -> &Any {
        self
    }

    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        Ok(box self.specialized_weight(searcher))
    }
}
