use query::Weight;
use query::Scorer;
use schema::Term;
use postings::SegmentPostingsOption;
use core::SegmentReader;
use super::PhraseScorer;
use postings::IntersectionDocSet;
use Result;

pub struct PhraseWeight {
    phrase_terms: Vec<Term>,
}

impl From<Vec<Term>> for PhraseWeight {
    fn from(phrase_terms: Vec<Term>) -> PhraseWeight {
        PhraseWeight {
            phrase_terms: phrase_terms
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let mut term_postings_list = Vec::new();
        for term in &self.phrase_terms {            
            let term_postings_option = reader.read_postings(term, SegmentPostingsOption::FreqAndPositions);
            if let Some(term_postings) = term_postings_option {
                term_postings_list.push(term_postings);
            }
        }
        let positions_offsets: Vec<u32> = (0u32..self.phrase_terms.len() as u32).collect();
        Ok(box PhraseScorer {
            intersection_docset: IntersectionDocSet::from(term_postings_list),
            positions_offsets: positions_offsets,
        })
    }
}
