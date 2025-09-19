use std::collections::HashMap;

use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::index::SegmentReader;
use crate::postings::FreqReadingOption;
use crate::query::disjunction::Disjunction;
use crate::query::explanation::does_not_match;
use crate::query::score_combiner::{DoNothingCombiner, ScoreCombiner};
use crate::query::term_query::TermScorer;
use crate::query::weight::{for_each_docset_buffered, for_each_pruning_scorer, for_each_scorer};
use crate::query::{
    intersect_scorers, BufferedUnionScorer, EmptyScorer, Exclude, Explanation, Occur,
    RequiredOptionalScorer, Scorer, Weight,
};
use crate::{DocId, Score};

enum SpecializedScorer {
    TermUnion(Vec<TermScorer>),
    Other(Box<dyn Scorer>),
}

fn scorer_disjunction<TScoreCombiner>(
    scorers: Vec<Box<dyn Scorer>>,
    score_combiner: TScoreCombiner,
    minimum_match_required: usize,
) -> Box<dyn Scorer>
where
    TScoreCombiner: ScoreCombiner,
{
    debug_assert!(!scorers.is_empty());
    debug_assert!(minimum_match_required > 1);
    if scorers.len() == 1 {
        return scorers.into_iter().next().unwrap(); // Safe unwrap.
    }
    Box::new(Disjunction::new(
        scorers,
        score_combiner,
        minimum_match_required,
    ))
}

fn scorer_union<TScoreCombiner>(
    scorers: Vec<Box<dyn Scorer>>,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    num_docs: u32,
) -> SpecializedScorer
where
    TScoreCombiner: ScoreCombiner,
{
    assert!(!scorers.is_empty());
    if scorers.len() == 1 {
        return SpecializedScorer::Other(scorers.into_iter().next().unwrap()); //< we checked the size beforehand
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
                // Block wand is only available if we read frequencies.
                return SpecializedScorer::TermUnion(scorers);
            } else {
                return SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
                    scorers,
                    score_combiner_fn,
                    num_docs,
                )));
            }
        }
    }
    SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
        scorers,
        score_combiner_fn,
        num_docs,
    )))
}

fn into_box_scorer<TScoreCombiner: ScoreCombiner>(
    scorer: SpecializedScorer,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    num_docs: u32,
) -> Box<dyn Scorer> {
    match scorer {
        SpecializedScorer::TermUnion(term_scorers) => {
            let union_scorer =
                BufferedUnionScorer::build(term_scorers, score_combiner_fn, num_docs);
            Box::new(union_scorer)
        }
        SpecializedScorer::Other(scorer) => scorer,
    }
}

/// Weight associated to the `BoolQuery`.
pub struct BooleanWeight<TScoreCombiner: ScoreCombiner> {
    weights: Vec<(Occur, Box<dyn Weight>)>,
    minimum_number_should_match: usize,
    scoring_enabled: bool,
    score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send>,
}

impl<TScoreCombiner: ScoreCombiner> BooleanWeight<TScoreCombiner> {
    /// Creates a new boolean weight.
    pub fn new(
        weights: Vec<(Occur, Box<dyn Weight>)>,
        scoring_enabled: bool,
        score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send + 'static>,
    ) -> BooleanWeight<TScoreCombiner> {
        BooleanWeight {
            weights,
            scoring_enabled,
            score_combiner_fn,
            minimum_number_should_match: 1,
        }
    }

    /// Create a new boolean weight with minimum number of required should clauses specified.
    pub fn with_minimum_number_should_match(
        weights: Vec<(Occur, Box<dyn Weight>)>,
        minimum_number_should_match: usize,
        scoring_enabled: bool,
        score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send + 'static>,
    ) -> BooleanWeight<TScoreCombiner> {
        BooleanWeight {
            weights,
            minimum_number_should_match,
            scoring_enabled,
            score_combiner_fn,
        }
    }

    fn per_occur_scorers(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<HashMap<Occur, Vec<Box<dyn Scorer>>>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<dyn Scorer>>> = HashMap::new();
        for (occur, subweight) in &self.weights {
            let sub_scorer: Box<dyn Scorer> = subweight.scorer(reader, boost)?;
            per_occur_scorers
                .entry(*occur)
                .or_default()
                .push(sub_scorer);
        }
        Ok(per_occur_scorers)
    }

    fn complex_scorer<TComplexScoreCombiner: ScoreCombiner>(
        &self,
        reader: &SegmentReader,
        boost: Score,
        score_combiner_fn: impl Fn() -> TComplexScoreCombiner,
    ) -> crate::Result<SpecializedScorer> {
        let num_docs = reader.num_docs();
        let mut per_occur_scorers = self.per_occur_scorers(reader, boost)?;
        // Indicate how should clauses are combined with other clauses.
        enum CombinationMethod {
            Ignored,
            // Only contributes to final score.
            Optional(SpecializedScorer),
            Required(SpecializedScorer),
        }
        let mut must_scorers = per_occur_scorers.remove(&Occur::Must);
        let should_opt = if let Some(mut should_scorers) = per_occur_scorers.remove(&Occur::Should)
        {
            let num_of_should_scorers = should_scorers.len();
            if self.minimum_number_should_match > num_of_should_scorers {
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
            }
            match self.minimum_number_should_match {
                0 => CombinationMethod::Optional(scorer_union(
                    should_scorers,
                    &score_combiner_fn,
                    num_docs,
                )),
                1 => {
                    let scorer_union = scorer_union(should_scorers, &score_combiner_fn, num_docs);
                    CombinationMethod::Required(scorer_union)
                }
                n if num_of_should_scorers == n => {
                    // When num_of_should_scorers equals the number of should clauses,
                    // they are no different from must clauses.
                    must_scorers = match must_scorers.take() {
                        Some(mut must_scorers) => {
                            must_scorers.append(&mut should_scorers);
                            Some(must_scorers)
                        }
                        None => Some(should_scorers),
                    };
                    CombinationMethod::Ignored
                }
                _ => CombinationMethod::Required(SpecializedScorer::Other(scorer_disjunction(
                    should_scorers,
                    score_combiner_fn(),
                    self.minimum_number_should_match,
                ))),
            }
        } else {
            // None of should clauses are provided.
            if self.minimum_number_should_match > 0 {
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
            } else {
                CombinationMethod::Ignored
            }
        };
        let exclude_scorer_opt: Option<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .map(|scorers| scorer_union(scorers, DoNothingCombiner::default, num_docs))
            .map(|specialized_scorer: SpecializedScorer| {
                into_box_scorer(specialized_scorer, DoNothingCombiner::default, num_docs)
            });
        let positive_scorer = match (should_opt, must_scorers) {
            (CombinationMethod::Ignored, Some(must_scorers)) => {
                SpecializedScorer::Other(intersect_scorers(must_scorers, num_docs))
            }
            (CombinationMethod::Optional(should_scorer), Some(must_scorers)) => {
                let must_scorer = intersect_scorers(must_scorers, num_docs);
                if self.scoring_enabled {
                    SpecializedScorer::Other(Box::new(
                        RequiredOptionalScorer::<_, _, TScoreCombiner>::new(
                            must_scorer,
                            into_box_scorer(should_scorer, &score_combiner_fn, num_docs),
                        ),
                    ))
                } else {
                    SpecializedScorer::Other(must_scorer)
                }
            }
            (CombinationMethod::Required(should_scorer), Some(mut must_scorers)) => {
                must_scorers.push(into_box_scorer(should_scorer, &score_combiner_fn, num_docs));
                SpecializedScorer::Other(intersect_scorers(must_scorers, num_docs))
            }
            (CombinationMethod::Ignored, None) => {
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)))
            }
            (CombinationMethod::Required(should_scorer), None) => should_scorer,
            // Optional options are promoted to required if no must scorers exists.
            (CombinationMethod::Optional(should_scorer), None) => should_scorer,
        };
        if let Some(exclude_scorer) = exclude_scorer_opt {
            let positive_scorer_boxed =
                into_box_scorer(positive_scorer, &score_combiner_fn, num_docs);
            Ok(SpecializedScorer::Other(Box::new(Exclude::new(
                positive_scorer_boxed,
                exclude_scorer,
            ))))
        } else {
            Ok(positive_scorer)
        }
    }
}

impl<TScoreCombiner: ScoreCombiner + Sync> Weight for BooleanWeight<TScoreCombiner> {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let num_docs = reader.num_docs();
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
            self.complex_scorer(reader, boost, &self.score_combiner_fn)
                .map(|specialized_scorer| {
                    into_box_scorer(specialized_scorer, &self.score_combiner_fn, num_docs)
                })
        } else {
            self.complex_scorer(reader, boost, DoNothingCombiner::default)
                .map(|specialized_scorer| {
                    into_box_scorer(specialized_scorer, DoNothingCombiner::default, num_docs)
                })
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

        let mut explanation = Explanation::new("BooleanClause. sum of ...", scorer.score());
        for (occur, subweight) in &self.weights {
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
        let num_docs = reader.num_docs();
        let scorer = self.complex_scorer(reader, 1.0, &self.score_combiner_fn)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer =
                    BufferedUnionScorer::build(term_scorers, &self.score_combiner_fn, num_docs);
                for_each_scorer(&mut union_scorer, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_scorer(scorer.as_mut(), callback);
            }
        }
        Ok(())
    }

    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> crate::Result<()> {
        let num_docs = reader.num_docs();
        let scorer = self.complex_scorer(reader, 1.0, || DoNothingCombiner)?;
        let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];

        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer =
                    BufferedUnionScorer::build(term_scorers, &self.score_combiner_fn, num_docs);
                for_each_docset_buffered(&mut union_scorer, &mut buffer, callback);
            }
            SpecializedScorer::Other(mut scorer) => {
                for_each_docset_buffered(scorer.as_mut(), &mut buffer, callback);
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
        let scorer = self.complex_scorer(reader, 1.0, &self.score_combiner_fn)?;
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
