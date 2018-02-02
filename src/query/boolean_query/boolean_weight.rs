use query::Weight;
use core::SegmentReader;
use query::EmptyScorer;
use query::Scorer;
use super::BooleanScorer;
use query::OccurFilter;
use query::Occur;
use Result;

pub struct BooleanWeight {
    weights: Vec<(Occur, Box<Weight>)>,
    scoring_disabled: bool
}

impl BooleanWeight {
    pub fn new(weights: Vec<(Occur, Box<Weight>)>, scoring_disabled: bool) -> BooleanWeight {
        BooleanWeight {
            weights,
            scoring_disabled
        }
    }
}

impl Weight for BooleanWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        if self.weights.is_empty() {
            Ok(box EmptyScorer)
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) =  &self.weights[0];
            if occur == Occur::MustNot {
                Ok(box EmptyScorer)
            } else {
                weight.scorer(reader)
            }
        } else {
            let sub_scorers: Vec<Box<Scorer + 'a>> = self.weights
                .iter()
                .map(|&(_, ref weight)| weight)
                .map(|weight| weight.scorer(reader))
                .collect::<Result<_>>()?;
            let occurs: Vec<Occur> = self.weights
                .iter()
                .map(|&(ref occur, _)| *occur)
                .collect();
            let occur_filter = OccurFilter::new(&occurs);
            let boolean_scorer = BooleanScorer::new(sub_scorers, occur_filter);
            Ok(box boolean_scorer)
        }

   }
}
