use query::Weight;
use query::Scorer;
use schema::Term;
use schema::IndexRecordOption;
use core::SegmentReader;
use super::PhraseScorer;
use query::EmptyScorer;
use Result;
use query::bm25::BM25Weight;

pub struct PhraseWeight {
    phrase_terms: Vec<Term>,
    similarity_weight: BM25Weight,
    score_needed: bool,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    pub fn new(phrase_terms: Vec<Term>,
               similarity_weight: BM25Weight,
               score_needed: bool) -> PhraseWeight {
        PhraseWeight {
            phrase_terms,
            similarity_weight,
            score_needed
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let similarity_weight = self.similarity_weight.clone();
        let field = self.phrase_terms[0].field();
        let fieldnorm_reader = reader.get_fieldnorms_reader(field);
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
            Ok(box PhraseScorer::new(term_postings_list, similarity_weight, fieldnorm_reader, self.score_needed))
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
            Ok(box PhraseScorer::new(term_postings_list, similarity_weight, fieldnorm_reader, self.score_needed))
        }
    }
}
