use core::SegmentReader;
use query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
use query::term_query::TermScorer;
use query::EmptyScorer;
use query::Exclude;
use query::Occur;
use query::RequiredOptionalScorer;
use query::Scorer;
use query::Union;
use query::Weight;
use query::{intersect_scorers, Explanation};
use std::collections::HashMap;
use SkipResult;
use {Result, TantivyError};

fn scorer_union<TScoreCombiner>(scorers: Vec<Box<Scorer>>) -> Box<Scorer>
where
    TScoreCombiner: ScoreCombiner,
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        return scorers.into_iter().next().unwrap(); //< we checked the size beforehands
    }

    {
        let is_all_term_queries = scorers.iter().all(|scorer| scorer.is::<TermScorer>());
        if is_all_term_queries {
            let scorers: Vec<TermScorer> = scorers
                .into_iter()
                .map(|scorer| *(scorer.downcast::<TermScorer>().map_err(|_| ()).unwrap()))
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

    fn per_occur_scorers(
        &self,
        reader: &SegmentReader,
    ) -> Result<HashMap<Occur, Vec<Box<Scorer>>>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<Scorer>>> = HashMap::new();
        for &(ref occur, ref subweight) in &self.weights {
            let sub_scorer: Box<Scorer> = subweight.scorer(reader)?;
            per_occur_scorers
                .entry(*occur)
                .or_insert_with(Vec::new)
                .push(sub_scorer);
        }
        Ok(per_occur_scorers)
    }

    fn complex_scorer<TScoreCombiner: ScoreCombiner>(
        &self,
        reader: &SegmentReader,
    ) -> Result<Box<Scorer>> {
        let mut per_occur_scorers = self.per_occur_scorers(reader)?;

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

    fn explain(&self, reader: &SegmentReader, doc: u32) -> Result<Explanation> {
        let mut scorer = self.scorer(reader)?;
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(TantivyError::InvalidArgument(
                "Document does not match".to_string(),
            ));
        }
        if !self.scoring_enabled {
            return Ok(Explanation::new(
                "BooleanQuery with no scoring".to_string(),
                1f32,
            ));
        }

        let mut explanation = Explanation::new("BooleanClause. Sum of ...", scorer.score());

        for &(ref occur, ref subweight) in &self.weights {
            if is_positive_occur(*occur) {
                if let Ok(child_explanation) = subweight.explain(reader, doc) {
                    explanation.set_child(format!("Occur {:?}", occur), child_explanation);
                }
            }
        }

        Ok(explanation)
    }
}

fn is_positive_occur(occur: Occur) -> bool {
    match occur {
        Occur::Must | Occur::Should => true,
        Occur::MustNot => false,
    }
}
