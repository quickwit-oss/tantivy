use Result;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use query::occur_filter::OccurFilter;
use postings::SegmentPostings;
use query::term_query::{TermWeight, TermScorer};
use query::boolean_query::BooleanScorer;

pub struct MultiTermWeight {
    pub weights: Vec<TermWeight>,
    pub occur_filter: OccurFilter,
}

impl MultiTermWeight {

    pub fn specialized_scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<BooleanScorer<TermScorer<SegmentPostings<'a>>>> {
        let mut term_scorers: Vec<TermScorer<_>> = Vec::new();
        for term_weight in &self.weights {
            let term_scorer = try!(term_weight.specialized_scorer(reader));
            term_scorers.push(term_scorer);
        }
        Ok(BooleanScorer::new(term_scorers, self.occur_filter))
    }
    
}

impl Weight for MultiTermWeight {
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        Ok(box try!(self.specialized_scorer(reader))) 
    }
}