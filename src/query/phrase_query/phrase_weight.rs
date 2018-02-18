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
    scoring_enabled: bool
}

impl PhraseWeight {
    pub fn new(phrase_terms: Vec<Term>, scoring_enabled: bool) -> PhraseWeight {
        PhraseWeight {
            phrase_terms,
            scoring_enabled // TODO compute the phrase freq if scoring is enabled. stop at first match else.
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
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
