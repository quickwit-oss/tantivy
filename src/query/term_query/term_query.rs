use super::term_weight::TermWeight;
use query::bm25::BM25Weight;
use query::Query;
use query::Weight;
use schema::IndexRecordOption;
use Result;
use Searcher;
use Term;

/// A Term query matches all of the documents
/// containing a specific term.
///
/// The score associated is defined as
/// `idf` *  sqrt(`term_freq` / `field norm`)
/// in which :
/// * `idf`        - inverse document frequency.
/// * `term_freq`  - number of occurrences of the term in the field
/// * `field norm` - number of tokens in the field.
#[derive(Clone, Debug)]
pub struct TermQuery {
    term: Term,
    index_record_option: IndexRecordOption,
}

impl TermQuery {
    /// Creates a new term query.
    pub fn new(term: Term, segment_postings_options: IndexRecordOption) -> TermQuery {
        TermQuery {
            term,
            index_record_option: segment_postings_options,
        }
    }

    pub fn term(&self) -> &Term {
        &self.term
    }

    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(&self, searcher: &Searcher, scoring_enabled: bool) -> TermWeight {
        let term = self.term.clone();
        let bm25_weight = BM25Weight::for_terms(searcher, &[term]);
        let index_record_option = if scoring_enabled {
            self.index_record_option
        } else {
            IndexRecordOption::Basic
        };
        TermWeight::new(self.term.clone(), index_record_option, bm25_weight)
    }
}

impl Query for TermQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(Box::new(self.specialized_weight(searcher, scoring_enabled)))
    }
}
