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
    intersect_scorers, AllScorer, BufferedUnionScorer, EmptyScorer, Exclude, Explanation, Occur,
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

/// num_docs is the number of documents in the segment.
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

enum ShouldScorersCombinationMethod {
    // Should scorers are irrelevant.
    Ignored,
    // Only contributes to final score.
    Optional(SpecializedScorer),
    // Regardless of score, the should scorers may impact whether a document is matching or not.
    Required(SpecializedScorer),
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

        // Indicate how should clauses are combined with must clauses.
        let mut must_scorers: Vec<Box<dyn Scorer>> =
            per_occur_scorers.remove(&Occur::Must).unwrap_or_default();
        let must_special_scorer_counts = remove_and_count_all_and_empty_scorers(&mut must_scorers);

        if must_special_scorer_counts.num_empty_scorers > 0 {
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let mut should_scorers = per_occur_scorers.remove(&Occur::Should).unwrap_or_default();
        let should_special_scorer_counts =
            remove_and_count_all_and_empty_scorers(&mut should_scorers);

        let mut exclude_scorers: Vec<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .unwrap_or_default();
        let exclude_special_scorer_counts =
            remove_and_count_all_and_empty_scorers(&mut exclude_scorers);

        if exclude_special_scorer_counts.num_all_scorers > 0 {
            // We exclude all documents at one point.
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let minimum_number_should_match = self
            .minimum_number_should_match
            .saturating_sub(should_special_scorer_counts.num_all_scorers);

        let should_scorers: ShouldScorersCombinationMethod = {
            let num_of_should_scorers = should_scorers.len();
            if minimum_number_should_match > num_of_should_scorers {
                // We don't have enough scorers to satisfy the minimum number of should matches.
                // The request will match no documents.
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
            }
            match minimum_number_should_match {
                0 if num_of_should_scorers == 0 => ShouldScorersCombinationMethod::Ignored,
                0 => ShouldScorersCombinationMethod::Optional(scorer_union(
                    should_scorers,
                    &score_combiner_fn,
                    num_docs,
                )),
                1 => ShouldScorersCombinationMethod::Required(scorer_union(
                    should_scorers,
                    &score_combiner_fn,
                    num_docs,
                )),
                n if num_of_should_scorers == n => {
                    // When num_of_should_scorers equals the number of should clauses,
                    // they are no different from must clauses.
                    must_scorers.append(&mut should_scorers);
                    ShouldScorersCombinationMethod::Ignored
                }
                _ => ShouldScorersCombinationMethod::Required(SpecializedScorer::Other(
                    scorer_disjunction(
                        should_scorers,
                        score_combiner_fn(),
                        self.minimum_number_should_match,
                    ),
                )),
            }
        };

        let exclude_scorer_opt: Option<Box<dyn Scorer>> = if exclude_scorers.is_empty() {
            None
        } else {
            let exclude_specialized_scorer: SpecializedScorer =
                scorer_union(exclude_scorers, DoNothingCombiner::default, num_docs);
            Some(into_box_scorer(
                exclude_specialized_scorer,
                DoNothingCombiner::default,
                num_docs,
            ))
        };

        let include_scorer = match (should_scorers, must_scorers) {
            (ShouldScorersCombinationMethod::Ignored, must_scorers) => {
                let boxed_scorer: Box<dyn Scorer> = if must_scorers.is_empty() {
                    // We do not have any should scorers, nor all scorers.
                    // There are still two cases here.
                    //
                    // If this follows the removal of some AllScorers in the should/must clauses,
                    // then we match all documents.
                    //
                    // Otherwise, it is really just an EmptyScorer.
                    if must_special_scorer_counts.num_all_scorers
                        + should_special_scorer_counts.num_all_scorers
                        > 0
                    {
                        Box::new(AllScorer::new(reader.max_doc()))
                    } else {
                        Box::new(EmptyScorer)
                    }
                } else {
                    intersect_scorers(must_scorers, num_docs)
                };
                SpecializedScorer::Other(boxed_scorer)
            }
            (ShouldScorersCombinationMethod::Optional(should_scorer), must_scorers) => {
                if must_scorers.is_empty() && must_special_scorer_counts.num_all_scorers == 0 {
                    // Optional options are promoted to required if no must scorers exists.
                    should_scorer
                } else {
                    let must_scorer = intersect_scorers(must_scorers, num_docs);
                    if self.scoring_enabled {
                        SpecializedScorer::Other(Box::new(RequiredOptionalScorer::<
                            _,
                            _,
                            TScoreCombiner,
                        >::new(
                            must_scorer,
                            into_box_scorer(should_scorer, &score_combiner_fn, num_docs),
                        )))
                    } else {
                        SpecializedScorer::Other(must_scorer)
                    }
                }
            }
            (ShouldScorersCombinationMethod::Required(should_scorer), mut must_scorers) => {
                if must_scorers.is_empty() {
                    should_scorer
                } else {
                    must_scorers.push(into_box_scorer(should_scorer, &score_combiner_fn, num_docs));
                    SpecializedScorer::Other(intersect_scorers(must_scorers, num_docs))
                }
            }
        };
        if let Some(exclude_scorer) = exclude_scorer_opt {
            let include_scorer_boxed =
                into_box_scorer(include_scorer, &score_combiner_fn, num_docs);
            Ok(SpecializedScorer::Other(Box::new(Exclude::new(
                include_scorer_boxed,
                exclude_scorer,
            ))))
        } else {
            Ok(include_scorer)
        }
    }
}

#[derive(Default, Copy, Clone, Debug)]
struct AllAndEmptyScorerCounts {
    num_all_scorers: usize,
    num_empty_scorers: usize,
}

fn remove_and_count_all_and_empty_scorers(
    scorers: &mut Vec<Box<dyn Scorer>>,
) -> AllAndEmptyScorerCounts {
    let mut counts = AllAndEmptyScorerCounts::default();
    scorers.retain(|scorer| {
        if scorer.is::<AllScorer>() {
            counts.num_all_scorers += 1;
            false
        } else if scorer.is::<EmptyScorer>() {
            counts.num_empty_scorers += 1;
            false
        } else {
            true
        }
    });
    counts
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
            if is_include_occur(*occur) {
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
        let scorer = self.complex_scorer(reader, 1.0, &self.score_combiner_fn)?;
        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer = BufferedUnionScorer::build(
                    term_scorers,
                    &self.score_combiner_fn,
                    reader.num_docs(),
                );
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
        let scorer = self.complex_scorer(reader, 1.0, || DoNothingCombiner)?;
        let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];

        match scorer {
            SpecializedScorer::TermUnion(term_scorers) => {
                let mut union_scorer = BufferedUnionScorer::build(
                    term_scorers,
                    &self.score_combiner_fn,
                    reader.num_docs(),
                );
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

fn is_include_occur(occur: Occur) -> bool {
    match occur {
        Occur::Must | Occur::Should => true,
        Occur::MustNot => false,
    }
}
