use query::Weight;
use query::Scorer;
use schema::Term;
use schema::IndexRecordOption;
use core::SegmentReader;
use super::PhraseScorer;
use query::EmptyScorer;
use Result;

pub struct PhraseWeight {
    phrase_terms: Vec<Term>,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    ///
    /// Right now `scoring_enabled` is actually ignored.
    /// In the future, disabling scoring will result in a small performance boost.
    // TODO use the scoring disable information to avoid compute the
    // phrase freq in that case, and compute the phrase freq when scoring is enabled.
    // Right now we never compute it :|
    pub fn new(phrase_terms: Vec<Term>, _scoring_enabled: bool) -> PhraseWeight {
        PhraseWeight { phrase_terms }
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        if reader.has_deletes() {
            let mut term_postings_list = Vec::new();
            for term in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings(term, IndexRecordOption::WithFreqsAndPositions) {
                    term_postings_list.push(postings);
                } else {
                    return Ok(box EmptyScorer);
                }
            }
            Ok(box PhraseScorer::new(term_postings_list))
        } else {
            let mut term_postings_list = Vec::new();
            for term in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings_no_deletes(term, IndexRecordOption::WithFreqsAndPositions) {
                    term_postings_list.push(postings);
                } else {
                    return Ok(box EmptyScorer);
                }
            }
            Ok(box PhraseScorer::new(term_postings_list))
        }
    }
}
