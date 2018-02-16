use query::Weight;
use core::SegmentReader;
use postings::{Intersection, Union};
use std::collections::HashMap;
use query::EmptyScorer;
use query::Scorer;
use query::Exclude;
use super::BooleanScorer;
use query::OccurFilter;
use query::ConstScorer;
use query::Occur;
use query::RequiredOptionalScorer;
use Result;


fn scorer_union<'a>(docsets: Vec<Box<Scorer + 'a>>) -> Box<Scorer + 'a> {
    assert!(!docsets.is_empty());
    if docsets.len() == 1 {
        docsets
            .into_iter()
            .next()
            .unwrap() //< we checked the size beforehands
    } else {
        // TODO have a UnionScorer instead.
        box ConstScorer::new(Union::from(docsets))
    }
}

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

    fn scorer_if_scoring_disabled<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<Scorer + 'a>>> = HashMap::new();
        for &(ref occur, ref subweight) in self.weights.iter() {
            let sub_scorer: Box<Scorer + 'a> = subweight.scorer(reader)?;
            per_occur_scorers
                .entry(*occur)
                .or_insert_with(Vec::new)
                .push(sub_scorer);
        }

        let should_scorer_opt: Option<Box<Scorer + 'a>> = per_occur_scorers
            .remove(&Occur::Should)
            .map(scorer_union);

        let exclude_scorer_opt: Option<Box<Scorer + 'a>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(scorer_union);

        let must_scorer_opt: Option<Box<Scorer + 'a>> = per_occur_scorers
            .remove(&Occur::Must)
            .map(|scorers| {
                let scorer: Box<Scorer> = box ConstScorer::new(Intersection::from(scorers));
                scorer
            });

        let positive_scorer: Box<Scorer> = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) =>
                box RequiredOptionalScorer::new(must_scorer, should_scorer),
            (None, Some(must_scorer)) =>
                must_scorer,
            (Some(should_scorer), None) =>
                should_scorer,
            (None, None) => {
                return Ok(box EmptyScorer);
            }
        };

        if let Some(exclude_scorer) = exclude_scorer_opt {
            Ok(box Exclude::new(positive_scorer, exclude_scorer))
        } else {
            Ok(positive_scorer)
        }
    }

    fn scorer_if_scoring_enabled<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
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
        } else if self.scoring_disabled {
            self.scorer_if_scoring_disabled(reader)
        } else {
            self.scorer_if_scoring_enabled(reader)
        }
   }
}
