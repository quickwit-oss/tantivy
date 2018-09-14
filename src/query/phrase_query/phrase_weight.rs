use super::PhraseScorer;
use core::SegmentReader;
use query::bm25::BM25Weight;
use query::EmptyScorer;
use query::Scorer;
use query::Weight;
use schema::IndexRecordOption;
use schema::Term;
use Result;

pub struct PhraseWeight {
    phrase_terms: Vec<(usize, Term)>,
    similarity_weight: BM25Weight,
    score_needed: bool,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    pub fn new(
        phrase_terms: Vec<(usize, Term)>,
        similarity_weight: BM25Weight,
        score_needed: bool,
    ) -> PhraseWeight {
        PhraseWeight {
            phrase_terms,
            similarity_weight,
            score_needed,
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let similarity_weight = self.similarity_weight.clone();
        let field = self.phrase_terms[0].1.field();
        let fieldnorm_reader = reader.get_fieldnorms_reader(field);
        if reader.has_deletes() {
            let mut term_postings_list = Vec::new();
            for &(offset, ref term) in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
                {
                    term_postings_list.push((offset, postings));
                } else {
                    return Ok(Box::new(EmptyScorer));
                }
            }
            Ok(Box::new(PhraseScorer::new(
                term_postings_list,
                similarity_weight,
                fieldnorm_reader,
                self.score_needed,
            )))
        } else {
            let mut term_postings_list = Vec::new();
            for &(offset, ref term) in &self.phrase_terms {
                if let Some(postings) = reader
                    .inverted_index(term.field())
                    .read_postings_no_deletes(&term, IndexRecordOption::WithFreqsAndPositions)
                {
                    term_postings_list.push((offset, postings));
                } else {
                    return Ok(Box::new(EmptyScorer));
                }
            }
            Ok(Box::new(PhraseScorer::new(
                term_postings_list,
                similarity_weight,
                fieldnorm_reader,
                self.score_needed,
            )))
        }
    }
}
