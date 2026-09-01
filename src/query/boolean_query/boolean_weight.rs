use std::cmp::Ordering;
use std::collections::HashMap;

use crate::docset::{SeekDangerResult, COLLECT_BLOCK_BUFFER_LEN};
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
use crate::{DocId, Score, TERMINATED};

enum SpecializedScorer {
    TermUnion(Vec<TermScorer>),
    TermIntersection(Vec<TermScorer>),
    Other(Box<dyn Scorer>),
}

impl SpecializedScorer {
    fn empty() -> Self {
        Self::Other(Box::new(EmptyScorer))
    }
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
    if scorers.len() == 1 && !scorers[0].is::<TermScorer>() {
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
            } else if scorers.len() == 1 {
                // Single TermScorer without freq reading — unwrap directly.
                return SpecializedScorer::Other(Box::new(scorers.into_iter().next().unwrap()));
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
        SpecializedScorer::TermUnion(mut term_scorers) => {
            if term_scorers.len() == 1 {
                Box::new(term_scorers.pop().unwrap())
            } else {
                let union_scorer =
                    BufferedUnionScorer::build(term_scorers, score_combiner_fn, num_docs);
                Box::new(union_scorer)
            }
        }
        SpecializedScorer::TermIntersection(term_scorers) => {
            let boxed_scorers: Vec<Box<dyn Scorer>> = term_scorers
                .into_iter()
                .map(|s| Box::new(s) as Box<dyn Scorer>)
                .collect();
            intersect_scorers(boxed_scorers, num_docs)
        }
        SpecializedScorer::Other(scorer) => scorer,
    }
}

/// Returns the effective MUST scorer, accounting for removed AllScorers.
///
/// All scorers in `must_scorers` must be valid and aligned on the same document.
///
/// When AllScorer instances are removed from must_scorers as an optimization,
/// we must restore the "match all" semantics if the list becomes empty.
fn effective_must_scorer(
    must_scorers: Vec<Box<dyn Scorer>>,
    removed_all_scorer_count: usize,
    max_doc: DocId,
    num_docs: u32,
) -> Option<Box<dyn Scorer>> {
    if must_scorers.is_empty() {
        if removed_all_scorer_count > 0 {
            // Had AllScorer(s) only - all docs match
            Some(Box::new(AllScorer::new(max_doc)))
        } else {
            // No MUST constraint at all
            None
        }
    } else {
        Some(intersect_scorers(must_scorers, num_docs))
    }
}

/// Returns a SHOULD scorer with AllScorer union if any were removed.
///
/// For union semantics (OR): if any SHOULD clause was an AllScorer, the result
/// should include all documents. We restore this by unioning with AllScorer.
///
/// When `scoring_enabled` is false, we can just return AllScorer alone since
/// we don't need score contributions from the should_scorer.
fn effective_should_scorer_for_union<TScoreCombiner: ScoreCombiner>(
    should_scorer: SpecializedScorer,
    removed_all_scorer_count: usize,
    max_doc: DocId,
    num_docs: u32,
    score_combiner_fn: impl Fn() -> TScoreCombiner,
    scoring_enabled: bool,
) -> SpecializedScorer {
    if removed_all_scorer_count > 0 {
        if scoring_enabled {
            // Need to union to get score contributions from both
            let all_scorers: Vec<Box<dyn Scorer>> = vec![
                into_box_scorer(should_scorer, &score_combiner_fn, num_docs),
                Box::new(AllScorer::new(max_doc)),
            ];
            SpecializedScorer::Other(Box::new(BufferedUnionScorer::build(
                all_scorers,
                score_combiner_fn,
                num_docs,
            )))
        } else {
            // Scoring disabled - AllScorer alone is sufficient
            SpecializedScorer::Other(Box::new(AllScorer::new(max_doc)))
        }
    } else {
        should_scorer
    }
}

/// Creates the scorers that will form an intersection in any order.
///
/// If successful, all returned scorers are valid and aligned on the same document.
///
/// The first scorer supplies the initial candidate. Every subsequent scorer is created with
/// `scorer_danger`, so implementations can avoid eagerly locating their first match and instead
/// start at the intersection's current candidate. If that candidate does not match, all scorers
/// created so far are advanced with `seek_danger` until they agree.
///
/// The returned Scorers are guaranteed to be in a "no-danger" state.
///
/// If the method identifies no doc in the intersection, we return a single empty scorer.
/// Semantically, it is different from returning a empty Vec, which is returned when weights was
/// empty.
fn create_aligned_scorers_for_intersection(
    weights: &[Box<dyn Weight>],
    reader: &SegmentReader,
    boost: Score,
) -> crate::Result<Vec<Box<dyn Scorer>>> {
    let mut candidate = 0u32;
    let mut scorers = Vec::with_capacity(weights.len());

    for weight in weights {
        let (seek_result, scorer) = weight.scorer_danger(reader, candidate, boost)?;
        scorers.push(scorer);

        if let SeekDangerResult::SeekLowerBound(new_candidate) = seek_result {
            debug_assert!(new_candidate > candidate);
            candidate = new_candidate;
        }

        if candidate >= TERMINATED {
            return Ok(vec![Box::new(EmptyScorer)]);
        }
    }

    // This eventually terminates because, at each iteration, the pair
    // (candidate, num_scorer_aligned) increases according to lexicographic order.
    //
    // Eventually, either candidate will reach TERMINATED or
    // num_scorer_aligned will reach scorers.len().
    let mut num_scorer_aligned = 0;
    for scorer_id in (0..scorers.len()).cycle() {
        if let SeekDangerResult::SeekLowerBound(seek_lower_bound) =
            scorers[scorer_id].seek_danger(candidate)
        {
            debug_assert!(candidate < seek_lower_bound);
            candidate = seek_lower_bound;
            if candidate == TERMINATED {
                return Ok(vec![Box::new(EmptyScorer)]);
            }
            num_scorer_aligned = 0;
        } else {
            num_scorer_aligned += 1;
            if num_scorer_aligned == scorers.len() {
                return Ok(scorers);
            }
        }
    }
    Ok(scorers)
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
    per_occur_weights: HashMap<Occur, Vec<Box<dyn Weight>>>,
    minimum_number_should_match: usize,
    scoring_enabled: bool,
    score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send>,
}

impl<TScoreCombiner: ScoreCombiner> BooleanWeight<TScoreCombiner> {
    /// Create a new boolean weight with minimum number of required should clauses specified.
    pub fn with_minimum_number_should_match(
        weights: Vec<(Occur, Box<dyn Weight>)>,
        minimum_number_should_match: usize,
        scoring_enabled: bool,
        score_combiner_fn: Box<dyn Fn() -> TScoreCombiner + Sync + Send + 'static>,
    ) -> BooleanWeight<TScoreCombiner> {
        let mut per_occur_weights: HashMap<Occur, Vec<Box<dyn Weight>>> = HashMap::default();
        for (occur, weight) in weights {
            per_occur_weights.entry(occur).or_default().push(weight);
        }

        // Optimisation: we rewrite the bool weight depending on the number of minimum should match
        // and the number of should clauses.
        let num_should_weights = per_occur_weights
            .get(&Occur::Should)
            .map(Vec::len)
            .unwrap_or(0);

        match num_should_weights.cmp(&minimum_number_should_match) {
            Ordering::Greater => {
                // nothing to do. We will need the minimum should match logic.
                BooleanWeight {
                    per_occur_weights,
                    minimum_number_should_match,
                    scoring_enabled,
                    score_combiner_fn,
                }
            }
            Ordering::Equal => {
                // Equal! All should clause will be required. We promote them to Must!
                let should_weights = per_occur_weights.remove(&Occur::Should);
                per_occur_weights
                    .entry(Occur::Must)
                    .or_default()
                    .extend(should_weights.into_iter().flatten());
                BooleanWeight {
                    per_occur_weights,
                    minimum_number_should_match: 0,
                    scoring_enabled,
                    score_combiner_fn,
                }
            }
            Ordering::Less => {
                // We will never be able to match the minimum should match threshold.
                // Let's return the empty weight
                BooleanWeight {
                    per_occur_weights: Default::default(),
                    minimum_number_should_match: 0,
                    scoring_enabled,
                    score_combiner_fn,
                }
            }
        }
    }

    fn complex_scorer<TComplexScoreCombiner: ScoreCombiner>(
        &self,
        reader: &SegmentReader,
        boost: Score,
        score_combiner_fn: impl Fn() -> TComplexScoreCombiner,
    ) -> crate::Result<SpecializedScorer> {
        let num_docs = reader.num_docs();

        let intersection_weights: &[Box<dyn Weight>] = self
            .per_occur_weights
            .get(&Occur::Must)
            .map(|weights| weights.as_slice())
            .unwrap_or(&[]);

        let required_scorers: Vec<Box<dyn Scorer>> =
            create_aligned_scorers_for_intersection(intersection_weights, reader, boost)?;

        let mut per_occur_scorers: HashMap<Occur, Vec<Box<dyn Scorer>>> = HashMap::new();

        if !required_scorers.is_empty() {
            // all scorer are aligned, it is ok to test a single one.
            if required_scorers[0].doc() >= TERMINATED {
                return Ok(SpecializedScorer::empty());
            }
            per_occur_scorers.insert(Occur::Must, required_scorers);
        }

        for occur in [Occur::MustNot, Occur::Should] {
            if let Some(occur_weights) = self.per_occur_weights.get(&occur) {
                let occur_scorers = per_occur_scorers.entry(occur).or_default();
                for occur_weight in occur_weights {
                    let occur_scorer = occur_weight.scorer(reader, boost)?;
                    occur_scorers.push(occur_scorer);
                }
            }
        }

        // Indicate how should clauses are combined with must clauses.
        let mut must_scorers: Vec<Box<dyn Scorer>> =
            per_occur_scorers.remove(&Occur::Must).unwrap_or_default();
        let must_special_scorer_counts: AllAndEmptyScorerCounts =
            remove_and_count_all_and_empty_scorers(&mut must_scorers);

        if must_special_scorer_counts.num_empty_scorers > 0 {
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let mut should_scorers = per_occur_scorers.remove(&Occur::Should).unwrap_or_default();
        let should_special_scorer_counts: AllAndEmptyScorerCounts =
            remove_and_count_all_and_empty_scorers(&mut should_scorers);

        let mut exclude_scorers: Vec<Box<dyn Scorer>> = per_occur_scorers
            .remove(&Occur::MustNot)
            .unwrap_or_default();
        let exclude_special_scorer_counts: AllAndEmptyScorerCounts =
            remove_and_count_all_and_empty_scorers(&mut exclude_scorers);

        if exclude_special_scorer_counts.num_all_scorers > 0 {
            // We exclude all documents at one point.
            return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
        }

        let effective_minimum_number_should_match = self
            .minimum_number_should_match
            .saturating_sub(should_special_scorer_counts.num_all_scorers);

        let should_scorers: ShouldScorersCombinationMethod = {
            let num_of_should_scorers = should_scorers.len();
            if effective_minimum_number_should_match > num_of_should_scorers {
                // We don't have enough scorers to satisfy the minimum number of should matches.
                // The request will match no documents.
                return Ok(SpecializedScorer::Other(Box::new(EmptyScorer)));
            }
            match effective_minimum_number_should_match {
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
                        effective_minimum_number_should_match,
                    ),
                )),
            }
        };

        let include_scorer = match (should_scorers, must_scorers) {
            (ShouldScorersCombinationMethod::Ignored, must_scorers) => {
                // No SHOULD clauses (or they were absorbed into MUST).
                // Result depends entirely on MUST + any removed AllScorers.
                let combined_all_scorer_count = must_special_scorer_counts.num_all_scorers
                    + should_special_scorer_counts.num_all_scorers;

                // Try to detect a pure TermScorer intersection for block-max optimization.
                // Preconditions: no removed AllScorers, at least 2 scorers, all TermScorer
                // with frequency reading enabled.
                if combined_all_scorer_count == 0
                    && must_scorers.len() >= 2
                    && must_scorers.iter().all(|s| s.is::<TermScorer>())
                {
                    let term_scorers: Vec<TermScorer> = must_scorers
                        .into_iter()
                        .map(|s| *(s.downcast::<TermScorer>().map_err(|_| ()).unwrap()))
                        .collect();
                    if term_scorers
                        .iter()
                        .all(|s| s.freq_reading_option() == FreqReadingOption::ReadFreq)
                    {
                        SpecializedScorer::TermIntersection(term_scorers)
                    } else {
                        let must_scorers: Vec<Box<dyn Scorer>> = term_scorers
                            .into_iter()
                            .map(|s| Box::new(s) as Box<dyn Scorer>)
                            .collect();
                        let boxed_scorer: Box<dyn Scorer> =
                            effective_must_scorer(must_scorers, 0, reader.max_doc(), num_docs)
                                .unwrap_or_else(|| Box::new(EmptyScorer));
                        SpecializedScorer::Other(boxed_scorer)
                    }
                } else {
                    let boxed_scorer: Box<dyn Scorer> = effective_must_scorer(
                        must_scorers,
                        combined_all_scorer_count,
                        reader.max_doc(),
                        num_docs,
                    )
                    .unwrap_or_else(|| Box::new(EmptyScorer));
                    SpecializedScorer::Other(boxed_scorer)
                }
            }
            (ShouldScorersCombinationMethod::Optional(should_scorer), must_scorers) => {
                // Optional SHOULD: contributes to scoring but not required for matching.
                match effective_must_scorer(
                    must_scorers,
                    must_special_scorer_counts.num_all_scorers,
                    reader.max_doc(),
                    num_docs,
                ) {
                    None => {
                        // No MUST constraint: promote SHOULD to required.
                        // Must preserve any removed AllScorers from SHOULD via union.
                        effective_should_scorer_for_union(
                            should_scorer,
                            should_special_scorer_counts.num_all_scorers,
                            reader.max_doc(),
                            num_docs,
                            &score_combiner_fn,
                            self.scoring_enabled,
                        )
                    }
                    Some(must_scorer) => {
                        // Has MUST constraint: SHOULD only affects scoring.
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
            }
            (ShouldScorersCombinationMethod::Required(should_scorer), must_scorers) => {
                // Required SHOULD: at least `minimum_number_should_match` must match.
                // Semantics: (MUST constraint) AND (SHOULD constraint)
                match effective_must_scorer(
                    must_scorers,
                    must_special_scorer_counts.num_all_scorers,
                    reader.max_doc(),
                    num_docs,
                ) {
                    None => {
                        // No MUST constraint: SHOULD alone determines matching.
                        should_scorer
                    }
                    Some(must_scorer) => {
                        // Has MUST constraint: intersect MUST with SHOULD.
                        let should_boxed =
                            into_box_scorer(should_scorer, &score_combiner_fn, num_docs);
                        SpecializedScorer::Other(intersect_scorers(
                            vec![must_scorer, should_boxed],
                            num_docs,
                        ))
                    }
                }
            }
        };
        if exclude_scorers.is_empty() {
            return Ok(include_scorer);
        }

        let include_scorer_boxed = into_box_scorer(include_scorer, &score_combiner_fn, num_docs);
        let scorer: Box<dyn Scorer> = if exclude_scorers.len() == 1 {
            let exclude_scorer = exclude_scorers.pop().unwrap();
            match exclude_scorer.downcast::<TermScorer>() {
                // Cast to TermScorer succeeded
                Ok(exclude_scorer) => Box::new(Exclude::new(include_scorer_boxed, *exclude_scorer)),
                // We get back the original Box<dyn Scorer>
                Err(exclude_scorer) => Box::new(Exclude::new(include_scorer_boxed, exclude_scorer)),
            }
        } else {
            Box::new(Exclude::new(include_scorer_boxed, exclude_scorers))
        };
        Ok(SpecializedScorer::Other(scorer))
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
        let num_sub_weights: usize = self.per_occur_weights.values().map(Vec::len).sum();
        if num_sub_weights == 0 {
            Ok(Box::new(EmptyScorer))
        } else if num_sub_weights == 1 {
            // We have single subscorer. Let's just just short circuit our logic and return it.
            let (occur, weight) = self
                .per_occur_weights
                .iter()
                .flat_map(|(occur, weights)| weights.iter().map(|weight| (*occur, weight)))
                .next()
                .unwrap();
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
        for (occur, subweights) in &self.per_occur_weights {
            for subweight in subweights {
                if is_include_occur(*occur) {
                    if let Ok(child_explanation) = subweight.explain(reader, doc) {
                        explanation.add_detail(child_explanation);
                    }
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
        let num_docs = reader.num_docs();
        match scorer {
            SpecializedScorer::TermUnion(mut term_scorers) => {
                if term_scorers.len() == 1 {
                    let mut term_scorer = term_scorers.pop().unwrap();
                    for_each_scorer(&mut term_scorer, callback);
                } else {
                    let mut union_scorer =
                        BufferedUnionScorer::build(term_scorers, &self.score_combiner_fn, num_docs);
                    for_each_scorer(&mut union_scorer, callback);
                }
            }
            SpecializedScorer::TermIntersection(term_scorers) => {
                let boxed_scorers: Vec<Box<dyn Scorer>> = term_scorers
                    .into_iter()
                    .map(|term_scorer| Box::new(term_scorer) as Box<dyn Scorer>)
                    .collect();
                let mut intersection = intersect_scorers(boxed_scorers, num_docs);
                for_each_scorer(intersection.as_mut(), callback);
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
        let num_docs = reader.num_docs();
        let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];

        match scorer {
            SpecializedScorer::TermUnion(mut term_scorers) => {
                if term_scorers.len() == 1 {
                    let mut term_scorer = term_scorers.pop().unwrap();
                    for_each_docset_buffered(&mut term_scorer, &mut buffer, callback);
                } else {
                    let mut union_scorer =
                        BufferedUnionScorer::build(term_scorers, &self.score_combiner_fn, num_docs);
                    for_each_docset_buffered(&mut union_scorer, &mut buffer, callback);
                }
            }
            SpecializedScorer::TermIntersection(term_scorers) => {
                let boxed_scorers: Vec<Box<dyn Scorer>> = term_scorers
                    .into_iter()
                    .map(|term_scorer| Box::new(term_scorer) as Box<dyn Scorer>)
                    .collect();
                let mut intersection = intersect_scorers(boxed_scorers, num_docs);
                for_each_docset_buffered(intersection.as_mut(), &mut buffer, callback);
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
        let num_docs = reader.num_docs();
        match scorer {
            // Block-WAND scores by summing the matching terms, so it may only
            // drive a combiner that sums. Anything else (dis_max) still needs
            // every matching term and falls back to the plain union.
            SpecializedScorer::TermUnion(term_scorers) => {
                if TScoreCombiner::SUPPORTS_BLOCK_WAND {
                    super::block_wand(term_scorers, threshold, callback);
                } else {
                    let mut union_scorer =
                        BufferedUnionScorer::build(term_scorers, &self.score_combiner_fn, num_docs);
                    for_each_pruning_scorer(&mut union_scorer, threshold, callback);
                }
            }
            // An intersection sums its term scores whatever the combiner:
            // `Intersection::score` hard-codes the sum and `into_box_scorer`
            // routes `TermIntersection` through it, so the combiner only ever
            // shapes how *should* clauses combine. Block-WAND's summing
            // therefore matches the unpruned scorer here for every combiner.
            SpecializedScorer::TermIntersection(term_scorers) => {
                super::block_wand_intersection(term_scorers, threshold, callback);
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

#[cfg(test)]
mod tests {
    use super::create_aligned_scorers_for_intersection;
    use crate::query::{EmptyScorer, EnableScoring, Query, TermQuery, Weight};
    use crate::schema::{Field, IndexRecordOption, Schema, TEXT};
    use crate::{Index, Searcher, Term, TERMINATED};

    fn test_index() -> crate::Result<(Index, Field)> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;

        for terms in [
            "a", "b", "a b", "c", "a c", "b c", "a b c", "a b", "a b c", "x", "y",
        ] {
            writer.add_document(doc!(text => terms))?;
        }
        writer.commit()?;

        Ok((index, text))
    }

    fn term_weight(
        searcher: &Searcher,
        field: Field,
        term: &str,
    ) -> crate::Result<Box<dyn Weight>> {
        TermQuery::new(Term::from_field_text(field, term), IndexRecordOption::Basic)
            .weight(EnableScoring::disabled_from_searcher(searcher))
    }

    #[test]
    fn test_create_aligned_scorers_for_empty_intersection() -> crate::Result<()> {
        let (index, _) = test_index()?;
        let searcher = index.reader()?.searcher();
        let weights: Vec<Box<dyn Weight>> = Vec::new();

        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert!(scorers.is_empty());
        Ok(())
    }

    #[test]
    fn test_create_aligned_scorers_for_single_weight() -> crate::Result<()> {
        let (index, text) = test_index()?;
        let searcher = index.reader()?.searcher();
        let weights = vec![term_weight(&searcher, text, "a")?];

        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorers.len(), 1);
        assert_eq!(scorers[0].doc(), 0);
        Ok(())
    }

    #[test]
    fn test_create_aligned_scorers_aligns_all_scorers() -> crate::Result<()> {
        let (index, text) = test_index()?;
        let searcher = index.reader()?.searcher();
        let weights = vec![
            term_weight(&searcher, text, "a")?,
            term_weight(&searcher, text, "b")?,
            term_weight(&searcher, text, "c")?,
        ];

        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorers.len(), 3);
        assert!(scorers.iter().all(|scorer| scorer.doc() == 6));
        Ok(())
    }

    #[test]
    fn test_create_aligned_scorers_returns_empty_scorer_for_disjoint_weights() -> crate::Result<()>
    {
        let (index, text) = test_index()?;
        let searcher = index.reader()?.searcher();
        let weights = vec![
            term_weight(&searcher, text, "x")?,
            term_weight(&searcher, text, "y")?,
        ];

        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorers.len(), 1);
        assert!(scorers[0].is::<EmptyScorer>());
        assert_eq!(scorers[0].doc(), TERMINATED);

        // We also test for the opposite order
        let weights = vec![
            term_weight(&searcher, text, "a")?,
            term_weight(&searcher, text, "missing")?,
        ];
        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorers.len(), 1);
        assert!(scorers[0].is::<EmptyScorer>());
        assert_eq!(scorers[0].doc(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_create_aligned_scorers_returns_empty_scorer_when_first_weight_is_empty(
    ) -> crate::Result<()> {
        let (index, text) = test_index()?;
        let searcher = index.reader()?.searcher();
        let weights = vec![
            term_weight(&searcher, text, "missing")?,
            term_weight(&searcher, text, "a")?,
        ];

        let scorers =
            create_aligned_scorers_for_intersection(&weights, searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorers.len(), 1);
        assert!(scorers[0].is::<EmptyScorer>());
        assert_eq!(scorers[0].doc(), TERMINATED);
        Ok(())
    }
}
