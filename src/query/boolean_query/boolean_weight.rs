use core::SegmentReader;
use downcast::Downcast;
use query::intersect_scorers;
use query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
use query::term_query::TermScorer;
use query::EmptyScorer;
use query::Exclude;
use query::Occur;
use query::RequiredOptionalScorer;
use query::Scorer;
use query::Union;
use query::Weight;
use std::borrow::Borrow;
use std::collections::HashMap;
use Result;

fn scorer_union<TScoreCombiner>(scorers: Vec<Box<Scorer>>) -> Box<Scorer>
where
    TScoreCombiner: ScoreCombiner,
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        return scorers.into_iter().next().unwrap(); //< we checked the size beforehands
    }

    {
        let is_all_term_queries = scorers.iter().all(|scorer| {
            let scorer_ref: &Scorer = scorer.borrow();
            Downcast::<TermScorer>::is_type(scorer_ref)
        });
        if is_all_term_queries {
            let scorers: Vec<TermScorer> = scorers
                .into_iter()
                .map(|scorer| *Downcast::<TermScorer>::downcast(scorer).unwrap())
                .collect();
            let scorer: Box<Scorer> = Box::new(Union::<TermScorer, TScoreCombiner>::from(scorers));
            return scorer;
        }
    }

    let scorer: Box<Scorer> = Box::new(Union::<_, TScoreCombiner>::from(scorers));
    scorer
}

pub struct BooleanWeight {
    weights: Vec<(Occur, Box<Weight>)>,
    scoring_enabled: bool,
}

impl BooleanWeight {
    pub fn new(weights: Vec<(Occur, Box<Weight>)>, scoring_enabled: bool) -> BooleanWeight {
        BooleanWeight {
            weights,
            scoring_enabled,
        }
    }

    fn complex_scorer<TScoreCombiner: ScoreCombiner>(
        &self,
        reader: &SegmentReader,
    ) -> Result<Box<Scorer>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<Scorer>>> = HashMap::new();
        for &(ref occur, ref subweight) in &self.weights {
            let sub_scorer: Box<Scorer> = subweight.scorer(reader)?;
            per_occur_scorers
                .entry(*occur)
                .or_insert_with(Vec::new)
                .push(sub_scorer);
        }

        let should_scorer_opt: Option<Box<Scorer>> = per_occur_scorers
            .remove(&Occur::Should)
            .map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(scorer_union::<TScoreCombiner>);

        let must_scorer_opt: Option<Box<Scorer>> = per_occur_scorers
            .remove(&Occur::Must)
            .map(intersect_scorers);

        let positive_scorer: Box<Scorer> = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) => {
                if self.scoring_enabled {
                    Box::new(RequiredOptionalScorer::<_, _, TScoreCombiner>::new(
                        must_scorer,
                        should_scorer,
                    ))
                } else {
                    must_scorer
                }
            }
            (None, Some(must_scorer)) => must_scorer,
            (Some(should_scorer), None) => should_scorer,
            (None, None) => {
                return Ok(Box::new(EmptyScorer));
            }
        };

        if let Some(exclude_scorer) = exclude_scorer_opt {
            Ok(Box::new(Exclude::new(positive_scorer, exclude_scorer)))
        } else {
            Ok(positive_scorer)
        }
    }
}

impl Weight for BooleanWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        if self.weights.is_empty() {
            Ok(Box::new(EmptyScorer))
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) = &self.weights[0];
            if occur == Occur::MustNot {
                Ok(Box::new(EmptyScorer))
            } else {
                weight.scorer(reader)
            }
        } else if self.scoring_enabled {
            self.complex_scorer::<SumWithCoordsCombiner>(reader)
        } else {
            self.complex_scorer::<DoNothingCombiner>(reader)
        }
    }
}
