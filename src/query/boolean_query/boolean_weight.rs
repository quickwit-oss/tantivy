use query::Weight;
use core::SegmentReader;
use query::Scorer;
use super::BooleanScorer;
use query::OccurFilter;
use Result;

pub struct BooleanWeight {
    weights: Vec<Box<Weight>>,
    occur_filter: OccurFilter,
}

impl BooleanWeight {
    pub fn new(weights: Vec<Box<Weight>>, occur_filter: OccurFilter) -> BooleanWeight {
        BooleanWeight {
            weights,
            occur_filter,
        }
    }
}


impl Weight for BooleanWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let sub_scorers: Vec<Box<Scorer + 'a>> = self.weights
            .iter()
            .map(|weight| weight.scorer(reader))
            .collect::<Result<_>>()?;
        let boolean_scorer = BooleanScorer::new(sub_scorers, self.occur_filter);
        Ok(box boolean_scorer)
    }
}
