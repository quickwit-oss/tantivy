use crate::core::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
use crate::query::term_query::TermScorer;
use crate::query::weight::{for_each_pruning_scorer, for_each_scorer};
use crate::query::EmptyScorer;
use crate::query::Exclude;
use crate::query::Occur;
use crate::query::RequiredOptionalScorer;
use crate::query::Scorer;
use crate::query::Weight;
use crate::query::{intersect_scorers, Explanation};
use crate::query::{TermUnion, Union};
use crate::{DocId, Score};
use std::collections::HashMap;

enum SpecializedScorer<TScoreCombiner: ScoreCombiner> {
    TermUnion(Union<TermScorer, TScoreCombiner>),
    Other(Box<dyn Scorer>),
}

fn scorer_union<TScoreCombiner>(scorers: Vec<Box<dyn Scorer>>) -> SpecializedScorer<TScoreCombiner>
where
    TScoreCombiner: ScoreCombiner,
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        return SpecializedScorer::Other(scorers.into_iter().next().unwrap()); //< we checked the size beforehands
    }

    {
        let is_all_term_queries = scorers.iter().all(|scorer| scorer.is::<TermScorer>());
        if is_all_term_queries {
            let scorers: Vec<TermScorer> = scorers
                .into_iter()
                .map(|scorer| *(scorer.downcast::<TermScorer>().map_err(|_| ()).unwrap()))
                .collect();
            return SpecializedScorer::TermUnion(Union::<TermScorer, TScoreCombiner>::from(
                scorers,
            ));
        }
    }
    SpecializedScorer::Other(Box::new(Union::<_, TScoreCombiner>::from(scorers)))
}

impl<TScoreCombiner: ScoreCombiner> Into<Box<dyn Scorer>> for SpecializedScorer<TScoreCombiner> {
    fn into(self) -> Box<dyn Scorer> {
        match self {
            Self::TermUnion(union) => Box::new(union),
            Self::Other(scorer) => scorer,
        }
    }
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
    ) -> crate::Result<SpecializedScorer<TScoreCombiner>> {
        let mut per_occur_scorers = self.per_occur_scorers(reader, boost)?;

        let should_scorer_opt: Option<SpecializedScorer<TScoreCombiner>> = per_occur_scorers
            .remove(&Occur::Should)
            .map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(scorer_union::<TScoreCombiner>)
            .map(Into::into);

        let must_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::Must)
            .map(intersect_scorers);

        let positive_scorer: SpecializedScorer<TScoreCombiner> =
            match (should_scorer_opt, must_scorer_opt) {
                (Some(should_scorer), Some(must_scorer)) => {
                    if self.scoring_enabled {
                        SpecializedScorer::Other(Box::new(RequiredOptionalScorer::<
                            Box<dyn Scorer>,
                            Box<dyn Scorer>,
                            TScoreCombiner,
                        >::new(
                            must_scorer, should_scorer.into()
                        )))
                    } else {
                        SpecializedScorer::Other(must_scorer)
                    }
                }
                (None, Some(must_scorer)) => SpecializedScorer::Other(must_scorer),
                (Some(should_scorer), None) => should_scorer,
                (None, None) => {
                    return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
                }
            };

        if let Some(exclude_scorer) = exclude_scorer_opt {
            let positive_scorer_boxed: Box<dyn Scorer> = positive_scorer.into();
            Ok(SpecializedScorer::Other(Box::new(Exclude::new(
                positive_scorer_boxed,
                exclude_scorer,
            ))))
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
                .map(Into::into)
        } else {
            self.complex_scorer::<DoNothingCombiner>(reader, boost)
                .map(Into::into)
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

    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer::<SumWithCoordsCombiner>(reader, 1.0f32)?;
        match scorer {
            SpecializedScorer::TermUnion(mut union_scorer) => {
                for_each_scorer(&mut union_scorer, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_scorer(scorer.as_mut(), callback);
            }
        }
        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the TopDocs collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: f32,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer::<SumWithCoordsCombiner>(reader, 1.0f32)?;
        match scorer {
            SpecializedScorer::TermUnion(mut union_scorer) => {
                for_each_pruning_scorer(&mut union_scorer, threshold, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_pruning_scorer(scorer.as_mut(), threshold, callback);
            }
        }
        Ok(())
    }
}

fn is_positive_occur(occur: Occur) -> bool {
    match occur {
        Occur::Must | Occur::Should => true,
        Occur::MustNot => false,
    }
}
