use crate::core::SegmentReader;
use crate::postings::FreqReadingOption;
use crate::query::explanation::does_not_match;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner, SumWithCoordsCombiner};
use crate::query::term_query::TermScorer;
use crate::query::weight::{for_each_pruning_scorer, for_each_scorer};
use crate::query::EmptyScorer;
use crate::query::Exclude;
use crate::query::Occur;
use crate::query::RequiredOptionalScorer;
use crate::query::Scorer;
use crate::query::Union;
use crate::query::Weight;
use crate::query::{intersect_scorers, Explanation};
use crate::{DocId, Score};
use std::collections::HashMap;

enum SpecializedScorer {
    TermUnion(Vec<TermScorer>),
    Other(Box<dyn Scorer>),
}

fn scorer_union<TScoreCombiner>(scorers: Vec<Box<dyn Scorer>>) -> SpecializedScorer
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
            if scorers
                .iter()
                .all(|scorer| scorer.freq_reading_option() == FreqReadingOption::ReadFreq)
            {
                // Block wand is only available iff we read frequencies.
                return SpecializedScorer::TermUnion(scorers);
            } else {
                return SpecializedScorer::Other(Box::new(Union::<_, TScoreCombiner>::from(
                    scorers,
                )));
            }
        }
    }
    SpecializedScorer::Other(Box::new(Union::<_, TScoreCombiner>::from(scorers)))
}

fn into_box_scorer<TScoreCombiner: ScoreCombiner>(scorer: SpecializedScorer) -> Box<dyn Scorer> {
    match scorer {
        SpecializedScorer::TermUnion(term_scorers) => {
            let union_scorer = Union::<TermScorer, TScoreCombiner>::from(term_scorers);
            Box::new(union_scorer)
        }
        SpecializedScorer::Other(scorer) => scorer,
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
        boost: Score,
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
        boost: Score,
    ) -> crate::Result<SpecializedScorer> {
        let mut per_occur_scorers = self.per_occur_scorers(reader, boost)?;

        let should_scorer_opt: Option<SpecializedScorer> = per_occur_scorers
            .remove(&Occur::Should)
            .map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(scorer_union::<DoNothingCombiner>)
            .map(into_box_scorer::<DoNothingCombiner>);

        let must_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::Must)
            .map(intersect_scorers);

        let positive_scorer: SpecializedScorer = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) => {
                if self.scoring_enabled {
                    SpecializedScorer::Other(Box::new(RequiredOptionalScorer::<
                        Box<dyn Scorer>,
                        Box<dyn Scorer>,
                        TScoreCombiner,
                    >::new(
                        must_scorer,
                        into_box_scorer::<TScoreCombiner>(should_scorer),
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
            let positive_scorer_boxed: Box<dyn Scorer> =
                into_box_scorer::<TScoreCombiner>(positive_scorer);
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
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
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
                .map(|specialized_scorer| {
                    into_box_scorer::<SumWithCoordsCombiner>(specialized_scorer)
                })
        } else {
            self.complex_scorer::<DoNothingCombiner>(reader, boost)
                .map(into_box_scorer::<DoNothingCombiner>)
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        if !self.scoring_enabled {
            return Ok(Explanation::new("BooleanQuery with no scoring", 1.0));
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
        let scorer = self.complex_scorer::<SumWithCoordsCombiner>(reader, 1.0)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer =
                    Union::<TermScorer, SumWithCoordsCombiner>::from(term_scorers);
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
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let scorer = self.complex_scorer::<SumWithCoordsCombiner>(reader, 1.0)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                super::block_wand(term_scorers, threshold, callback);
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
