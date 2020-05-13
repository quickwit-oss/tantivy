use crate::core::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
use crate::query::term_query::TermScorer;
use crate::query::EmptyScorer;
use crate::query::Exclude;
use crate::query::Occur;
use crate::query::RequiredOptionalScorer;
use crate::query::Scorer;
use crate::query::Union;
use crate::query::Weight;
use crate::query::{intersect_scorers, Explanation};
use crate::DocId;
use std::collections::HashMap;

fn scorer_union<TScoreCombiner>(scorers: Vec<Box<dyn Scorer>>) -> Box<dyn Scorer>
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
            let scorer: Box<dyn Scorer> =
                Box::new(Union::<TermScorer, TScoreCombiner>::from(scorers));
            return scorer;
        }
    }

    let scorer: Box<dyn Scorer> = Box::new(Union::<_, TScoreCombiner>::from(scorers));
    scorer
}

pub struct BooleanWeight {
    weights: Vec<(Occur, Box<dyn Weight>)>,
    scoring_enabled: bool,
}

impl BooleanWeight {
    pub fn new(weights: Vec<(Occur, Box<dyn Weight>)>, scoring_enabled: bool) -> BooleanWeight {
        BooleanWeight {
            weights,
            scoring_enabled,
        }
    }

    fn per_occur_scorers(
        &self,
        reader: &SegmentReader,
        boost: f32,
    ) -> crate::Result<HashMap<Occur, Vec<Box<dyn Scorer>>>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<dyn Scorer>>> = HashMap::new();
        for &(ref occur, ref subweight) in &self.weights {
            let sub_scorer: Box<dyn Scorer> = subweight.scorer(reader, boost)?;
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
        boost: f32,
    ) -> crate::Result<Box<dyn Scorer>> {
        let mut per_occur_scorers = self.per_occur_scorers(reader, boost)?;

        let should_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::Should)
            .map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(scorer_union::<TScoreCombiner>);

        let must_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::Must)
            .map(intersect_scorers);

        let positive_scorer: Box<dyn Scorer> = match (should_scorer_opt, must_scorer_opt) {
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
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>> {
        if self.weights.is_empty() {
            Ok(Box::new(EmptyScorer))
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) = &self.weights[0];
            if occur == Occur::MustNot {
                Ok(Box::new(EmptyScorer))
            } else {
                weight.scorer(reader, boost)
            }
        } else if self.scoring_enabled {
            self.complex_scorer::<SumWithCoordsCombiner>(reader, boost)
        } else {
            self.complex_scorer::<DoNothingCombiner>(reader, boost)
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0f32)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        if !self.scoring_enabled {
            return Ok(Explanation::new("BooleanQuery with no scoring", 1f32));
        }

        let mut explanation = Explanation::new("BooleanClause. Sum of ...", scorer.score());
        for &(ref occur, ref subweight) in &self.weights {
            if is_positive_occur(*occur) {
                if let Ok(child_explanation) = subweight.explain(reader, doc) {
                    explanation.add_detail(child_explanation);
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
