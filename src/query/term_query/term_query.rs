use Term;
use Result;
use super::term_weight::TermWeight;
use query::Query;
use query::Weight;
use schema::IndexRecordOption;
use Searcher;

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

    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(&self, searcher: &Searcher, scoring_enabled: bool) -> TermWeight {
        let mut total_num_tokens = 0;
        let mut total_num_docs = 0;
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(self.term.field());
            total_num_tokens += inverted_index.total_num_tokens();
            total_num_docs += segment_reader.max_doc();
        }
        let average_field_norm = total_num_tokens as f32 / total_num_docs as f32;

        let index_record_option = if scoring_enabled {
            self.index_record_option
        } else {
            IndexRecordOption::Basic
        };
        TermWeight::new(
            searcher.doc_freq(&self.term),
            searcher.num_docs(),
            average_field_norm,
            self.term.clone(),
            index_record_option
        )
    }
}

impl Query for TermQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>> {
        Ok(box self.specialized_weight(searcher, scoring_enabled))
    }
}

