use query::Weight;
use core::SegmentReader;
use postings::{IntersectionDocSet, UnionDocSet};
use std::collections::HashMap;
use query::EmptyScorer;
use query::Scorer;
use super::BooleanScorer;
use query::OccurFilter;
use query::ConstScorer;
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
            let &(occur, ref weight) = &self.weights[0];
                if occur == Occur::MustNot {
                Ok(box EmptyScorer)
            } else {
                weight.scorer(reader)
            }
        } else {
            if self.scoring_disabled {
                let mut per_occur_scorers = HashMap::new();
                for &(ref occur, ref subweight) in &self.weights {
                    per_occur_scorers
                        .entry(occur)
                        .or_insert_with(Vec::new)
                        .push(subweight.scorer(reader)?);
                }
                let mut result_scorer_opt: Option<Box<Scorer>> = per_occur_scorers
                    .remove(&Occur::Should)
                    .map(|subscorers| {
                        assert!(!subscorers.is_empty());
                        if subscorers.len() == 1 {
                            subscorers
                                .into_iter()
                                .next()
                                .unwrap() //< we checked the size beforehands
                        } else {
                            box ConstScorer::new(UnionDocSet::from(subscorers))
                        }
                    });
                if let Some(mut subscorers) = per_occur_scorers.remove(&Occur::Must) {
                    if let Some(should_query) = result_scorer_opt {
                        subscorers.push(should_query);
                    }
                    let intersection_docset = IntersectionDocSet::from(subscorers);
                    result_scorer_opt = Some(box ConstScorer::new(intersection_docset));
                }

                if let Some(result_scorer) = result_scorer_opt {
                    Ok(result_scorer)
                } else {
                    Ok(box EmptyScorer)
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
}
