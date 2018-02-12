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

impl From<Vec<Term>> for PhraseWeight {
    fn from(phrase_terms: Vec<Term>) -> PhraseWeight {
        PhraseWeight { phrase_terms }
    }
}

impl Weight for PhraseWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let mut term_postings_list = Vec::new();
        for term in &self.phrase_terms {
            if let Some(postings) = reader
                .inverted_index(term.field())
                .read_postings(term, IndexRecordOption::WithFreqsAndPositions)
            {
                term_postings_list.push(postings);
            } else {
                return Ok(box EmptyScorer);
            }
        }
        Ok(box PhraseScorer::new(term_postings_list))
    }
}
