use query::Weight;
use core::SegmentReader;
use query::{Intersection, Union};
use std::collections::HashMap;
use query::EmptyScorer;
use query::Scorer;
use downcast::Downcast;
use query::term_query::TermScorer;
use std::borrow::Borrow;
use query::Exclude;
use query::Occur;
use query::RequiredOptionalScorer;
use query::IntersectionTwoTerms;
use fastfield::DeleteBitSet;
use postings::NoDelete;
use query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
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
            Downcast::<TermScorer<DeleteBitSet>>::is_type(scorer_ref)
        });
        if is_all_term_queries {
            let scorers: Vec<TermScorer<DeleteBitSet>> = scorers
                .into_iter()
                .map(|scorer| *Downcast::<TermScorer<DeleteBitSet>>::downcast(scorer).unwrap())
                .collect();
            let scorer: Box<Scorer> = box Union::<TermScorer<DeleteBitSet>, TScoreCombiner>::from(scorers);
            return scorer;
        }
    }

    {
        let is_all_term_queries = scorers.iter().all(|scorer| {
            let scorer_ref: &Scorer = scorer.borrow();
            Downcast::<TermScorer<NoDelete>>::is_type(scorer_ref)
        });
        if is_all_term_queries {
            let scorers: Vec<TermScorer<NoDelete>> = scorers
                .into_iter()
                .map(|scorer| *Downcast::<TermScorer<NoDelete>>::downcast(scorer).unwrap())
                .collect();
            let scorer: Box<Scorer> = box Union::<TermScorer<NoDelete>, TScoreCombiner>::from(scorers);
            return scorer;
        }
    }
    let scorer: Box<Scorer> = box Union::<_, TScoreCombiner>::from(scorers);
    return scorer;

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

        let must_scorer_opt: Option<Box<Scorer>> =
            per_occur_scorers.remove(&Occur::Must).map(|mut scorers| {
                if scorers.len() == 1 {
                    return scorers.into_iter().next().unwrap();
                }
                scorers.sort_by_key(|scorer| scorer.size_hint());
                {
                    let is_all_term_queries = scorers.iter().all(|scorer| {
                        let scorer_ref: &Scorer = scorer.borrow();
                        Downcast::<TermScorer<DeleteBitSet>>::is_type(scorer_ref)
                    });
                    if is_all_term_queries {
                        if scorers.len() == 2 {
                            let right = scorers.pop().unwrap();
                            let left = scorers.pop().unwrap();
                            return box IntersectionTwoTerms::new(left, right);
                        } else {
                            let mut scorers: Vec<TermScorer<DeleteBitSet>> = scorers
                                .into_iter()
                                .map(|scorer| *Downcast::<TermScorer<DeleteBitSet>>::downcast(scorer).unwrap())
                                .collect();
                            let scorer: Box<Scorer> = box Intersection::from(scorers);
                            return scorer;
                        }
                    }
                }
                {
                    let is_all_term_queries = scorers.iter().all(|scorer| {
                        let scorer_ref: &Scorer = scorer.borrow();
                        Downcast::<TermScorer<NoDelete>>::is_type(scorer_ref)
                    });
                    if is_all_term_queries {
                        if scorers.len() == 2 {
                            let right = scorers.pop().unwrap();
                            let left = scorers.pop().unwrap();
                            return box IntersectionTwoTerms::new(left, right);
                        } else {
                            let mut scorers: Vec<TermScorer<NoDelete>> = scorers
                                .into_iter()
                                .map(|scorer| *Downcast::<TermScorer<NoDelete>>::downcast(scorer).unwrap())
                                .collect();
                            let scorer: Box<Scorer> = box Intersection::from(scorers);
                            return scorer;
                        }
                    }
                }
                {
                    let scorer: Box<Scorer> = box Intersection::from(scorers);
                    scorer
                }
            });

        let positive_scorer: Box<Scorer> = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) => {
                if self.scoring_enabled {
                    box RequiredOptionalScorer::<_, _, TScoreCombiner>::new(
                        must_scorer,
                        should_scorer,
                    )
                } else {
                    must_scorer
                }
            }
            (None, Some(must_scorer)) => must_scorer,
            (Some(should_scorer), None) => should_scorer,
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
}

impl Weight for BooleanWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        if self.weights.is_empty() {
            Ok(box EmptyScorer)
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) = &self.weights[0];
            if occur == Occur::MustNot {
                Ok(box EmptyScorer)
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
