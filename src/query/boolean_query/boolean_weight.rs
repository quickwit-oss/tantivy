use query::Weight;
use core::SegmentReader;
use postings::{Intersection, Union};
use std::collections::HashMap;
use query::EmptyScorer;
use query::Scorer;
use downcast::Downcast;
use query::term_query::TermScorer;
use std::borrow::Borrow;
use query::Exclude;
use query::Occur;
use query::RequiredOptionalScorer;
use query::score_combiner::{SumWithCoordsCombiner, DoNothingCombiner, ScoreCombiner};
use Result;

fn scorer_union<'a, TScoreCombiner>(scorers: Vec<Box<Scorer + 'a>>) -> Box<Scorer + 'a>
    where TScoreCombiner: ScoreCombiner + 'static
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        scorers.into_iter().next().unwrap() //< we checked the size beforehands
    } else {
        if scorers
            .iter()
            .all(|scorer| {
                let scorer_ref:&Scorer = scorer.borrow();
                Downcast::<TermScorer>::is_type(scorer_ref)
            }) {
            let scorers: Vec<TermScorer> = scorers.into_iter()
                .map(|scorer| {
                    *Downcast::<TermScorer>::downcast(scorer)
                        .expect("downcasting should not have failed, we\
                                    checked in advance that the type were correct.")
                })
                .collect();
            let scorer: Box<Scorer> = box Union::<TermScorer, TScoreCombiner>::from(scorers);
            scorer
        } else {
            let scorer: Box<Scorer> = box Union::<_, TScoreCombiner>::from(scorers);
            scorer
        }
    }
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
        for &(ref occur, ref subweight) in self.weights.iter() {
            let sub_scorer: Box<Scorer> = subweight.scorer(reader)?;
            per_occur_scorers
                .entry(*occur)
                .or_insert_with(Vec::new)
                .push(sub_scorer);
        }

        let should_scorer_opt: Option<Box<Scorer>> =
            per_occur_scorers.remove(&Occur::Should).map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<Scorer>> =
            per_occur_scorers.remove(&Occur::MustNot).map(scorer_union::<TScoreCombiner>);

        let must_scorer_opt: Option<Box<Scorer>> =
            per_occur_scorers.remove(&Occur::Must).map(|scorers| {
                if scorers.len() == 1 {
                    scorers.into_iter().next().unwrap()
                } else {
                    if scorers
                        .iter()
                        .all(|scorer| {
                            let scorer_ref:&Scorer = scorer.borrow();
                            Downcast::<TermScorer>::is_type(scorer_ref)
                        }) {
                        let scorers: Vec<TermScorer> = scorers.into_iter()
                            .map(|scorer| {
                                *Downcast::<TermScorer>::downcast(scorer)
                                    .expect("downcasting should not have failed, we\
                                    checked in advance that the type were correct.")

                            })
                            .collect();
                        let scorer: Box<Scorer> = box Intersection::from(scorers);
                        scorer
                    } else {
                        let scorer: Box<Scorer> = box Intersection::from(scorers);
                        scorer
                    }
                }
            });

        let positive_scorer: Box<Scorer > = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) => {
                if self.scoring_enabled {
                    box RequiredOptionalScorer::<_,_,TScoreCombiner>::new(must_scorer, should_scorer)
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
