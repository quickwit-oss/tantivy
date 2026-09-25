use std::cmp::Ordering;
use std::sync::Arc;

mod function_predicate;
#[cfg(feature = "jitexpr")]
mod jitexpr_predicate;

pub use function_predicate::FunctionPredicate;
#[cfg(feature = "jitexpr")]
pub use jitexpr_predicate::{JitExprEvalState, JitExprPredicate};

use crate::docset::{SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::{
    AllScorer, AllWeight, ConstScorer, EmptyWeight, EnableScoring, Explanation, Query, Scorer,
    Weight,
};
use crate::{DocId, DocSet, Score};

/// A query that evaluates, for each DocId, whether it matches or not.
#[derive(Clone, Debug)]
pub struct DocPredicateQuery {
    predicate: Arc<dyn DocPredicateBoxable>,
}

impl From<Arc<dyn DocPredicateBoxable>> for DocPredicateQuery {
    fn from(predicate: Arc<dyn DocPredicateBoxable>) -> Self {
        DocPredicateQuery { predicate }
    }
}

impl<TDocPredicateBoxable: DocPredicateBoxable> From<TDocPredicateBoxable> for DocPredicateQuery {
    fn from(predicate: TDocPredicateBoxable) -> Self {
        DocPredicateQuery {
            predicate: Arc::new(predicate),
        }
    }
}

impl Query for DocPredicateQuery {
    fn weight(&self, _enable_scoring: EnableScoring) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.clone()))
    }
}

impl Weight for DocPredicateQuery {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        self.predicate.scorer(reader, boost)
    }

    fn scorer_danger(
        &self,
        reader: &SegmentReader,
        target: DocId,
        boost: Score,
    ) -> crate::Result<(SeekDangerResult, Box<dyn Scorer>)> {
        self.predicate.scorer_danger(reader, target, boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let (seek_result, _docset) = self.predicate.scorer_danger(reader, doc, 1.0f32)?;
        if let SeekDangerResult::SeekLowerBound(_) = seek_result {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("CalculatedPredicateQuery", 1.0))
    }
}

/// The cost of a [`DocPredicateDocSet`] is the cost of its necessary condition,
/// multiplied by this factor.
const PREDICATE_EVAL_COST_FACTOR: u64 = 100;

/// A [`DocSet`] that walks the documents of a necessary condition, and evaluates a
/// [`SegmentDocPredicate`] on each of them.
///
/// Hidden contract: every document matching the predicate belongs to the necessary condition.
/// Documents outside of it are never evaluated, and are considered as not matching.
///
/// Hidden contract: whenever the `DocPredicateDocSet` is in a valid state, the necessary condition
/// is in a valid state too, positioned on `self.doc`.
pub struct DocPredicateDocSet<TSegmentDocPredicate> {
    doc_predicate: TSegmentDocPredicate,
    necessary_condition: Box<dyn DocSet>,
    doc: DocId,
}

impl<TSegmentDocPredicate: SegmentDocPredicate> DocPredicateDocSet<TSegmentDocPredicate> {
    /// Creates a `DocPredicateDocSet` positioned on its first matching document.
    fn new(doc_predicate: TSegmentDocPredicate, necessary_condition: Box<dyn DocSet>) -> Self {
        let first_candidate = necessary_condition.doc();
        let mut doc_set = DocPredicateDocSet {
            doc_predicate,
            necessary_condition,
            doc: first_candidate,
        };
        doc_set.find_match(first_candidate);
        doc_set
    }

    /// Creates a `DocPredicateDocSet`, and seeks it to `target`, following
    /// [`Weight::scorer_danger`]'s contract.
    ///
    /// Documents before `target` are not evaluated.
    fn new_seeked_to(
        doc_predicate: TSegmentDocPredicate,
        necessary_condition: Box<dyn DocSet>,
        target: DocId,
    ) -> (SeekDangerResult, Self) {
        let first_candidate = necessary_condition.doc();
        let mut doc_set = DocPredicateDocSet {
            doc_predicate,
            necessary_condition,
            doc: first_candidate,
        };
        let seek_result = match first_candidate.cmp(&target) {
            Ordering::Less => doc_set.seek_danger(target),
            Ordering::Equal => doc_set.eval_candidate(target),
            Ordering::Greater => SeekDangerResult::SeekLowerBound(first_candidate),
        };
        (seek_result, doc_set)
    }

    /// Evaluates the predicate on `candidate`.
    ///
    /// Hidden contract: the necessary condition is positioned on `candidate`.
    fn eval_candidate(&mut self, candidate: DocId) -> SeekDangerResult {
        if self.doc_predicate.eval(candidate) {
            self.doc = candidate;
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(candidate + 1)
        }
    }

    /// Advances to the first matching document at or after `candidate`.
    ///
    /// Hidden contract: the necessary condition is positioned on `candidate`.
    fn find_match(&mut self, mut candidate: DocId) -> DocId {
        while candidate != TERMINATED && !self.doc_predicate.eval(candidate) {
            candidate = self.necessary_condition.advance();
        }
        self.doc = candidate;
        candidate
    }
}

impl<TSegmentDocPredicate: SegmentDocPredicate> DocSet
    for DocPredicateDocSet<TSegmentDocPredicate>
{
    fn advance(&mut self) -> DocId {
        if self.doc == TERMINATED {
            return TERMINATED;
        }
        let candidate = self.necessary_condition.advance();
        self.find_match(candidate)
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc);
        // In a valid state, `self.doc` is a match (or TERMINATED).
        if self.doc >= target {
            return self.doc;
        }
        let candidate = self.necessary_condition.seek(target);
        self.find_match(candidate)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        match self.necessary_condition.seek_danger(target) {
            SeekDangerResult::Found => self.eval_candidate(target),
            SeekDangerResult::SeekLowerBound(lower_bound) => {
                if lower_bound == TERMINATED {
                    self.doc = TERMINATED;
                }
                SeekDangerResult::SeekLowerBound(lower_bound)
            }
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.necessary_condition.size_hint()
    }

    fn cost(&self) -> u64 {
        self.necessary_condition
            .cost()
            .saturating_mul(PREDICATE_EVAL_COST_FACTOR)
    }
}

/// A dyn-safe, type-erased [`DocPredicate`].
pub trait DocPredicateBoxable: std::fmt::Debug + 'static + Send + Sync {
    /// Builds a [`Scorer`] over the predicate's matching documents in the
    /// given segment.
    fn scorer(&self, segment_reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>>;

    /// Builds a [`Scorer`] seeked to `target`, following
    /// [`Weight::scorer_danger`]'s contract.
    fn scorer_danger(
        &self,
        segment_reader: &SegmentReader,
        target: DocId,
        boost: f32,
    ) -> crate::Result<(SeekDangerResult, Box<dyn Scorer>)>;
}

impl<TDocPredicate: DocPredicate> DocPredicateBoxable for TDocPredicate {
    fn scorer(&self, segment_reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>> {
        let const_or_variable_segment_predicate = self.doc_predicate(segment_reader)?;
        match const_or_variable_segment_predicate {
            ConstOrVariableSegmentPredicate::Const(always_match) => {
                if always_match {
                    AllWeight.scorer(segment_reader, boost)
                } else {
                    EmptyWeight.scorer(segment_reader, boost)
                }
            }
            ConstOrVariableSegmentPredicate::Variable {
                predicate,
                necessary_condition,
            } => {
                let necessary_condition = necessary_condition
                    .unwrap_or_else(|| Box::new(AllScorer::new(segment_reader.max_doc())));
                let doc_set = DocPredicateDocSet::new(predicate, necessary_condition);
                Ok(Box::new(ConstScorer::new(doc_set, boost)))
            }
        }
    }

    fn scorer_danger(
        &self,
        segment_reader: &SegmentReader,
        target: DocId,
        boost: f32,
    ) -> crate::Result<(SeekDangerResult, Box<dyn Scorer>)> {
        let const_or_variable_segment_predicate = self.doc_predicate(segment_reader)?;
        match const_or_variable_segment_predicate {
            ConstOrVariableSegmentPredicate::Const(always_match) => {
                if always_match {
                    AllWeight.scorer_danger(segment_reader, target, boost)
                } else {
                    EmptyWeight.scorer_danger(segment_reader, target, boost)
                }
            }
            ConstOrVariableSegmentPredicate::Variable {
                predicate,
                necessary_condition,
            } => {
                let necessary_condition = necessary_condition
                    .unwrap_or_else(|| Box::new(AllScorer::new(segment_reader.max_doc())));
                let (seek_result, doc_set) =
                    DocPredicateDocSet::new_seeked_to(predicate, necessary_condition, target);
                Ok((seek_result, Box::new(ConstScorer::new(doc_set, boost))))
            }
        }
    }
}

/// Represents a segment predicate.
pub enum ConstOrVariableSegmentPredicate<P: SegmentDocPredicate> {
    /// Can be emitted to hint that a predicate will be always true or false on a segment.
    /// Returning Const instead of a variable is an optimization.
    Const(bool),
    /// A regular SegmentDocPredicate, evaluated document by document.
    Variable {
        /// The predicate to evaluate.
        predicate: P,
        /// An optional [`DocSet`] restricting the documents on which `predicate` is evaluated.
        ///
        /// Hidden contract: it must contain every document for which `predicate.eval` returns
        /// true. Documents outside of it are never evaluated, and are considered as not
        /// matching. `None` means that every document of the segment must be evaluated.
        ///
        /// The `DocSet` must be positioned on its first document.
        necessary_condition: Option<Box<dyn DocSet>>,
    },
}

impl<P: SegmentDocPredicate> From<P> for ConstOrVariableSegmentPredicate<P> {
    fn from(predicate: P) -> Self {
        ConstOrVariableSegmentPredicate::Variable {
            predicate,
            necessary_condition: None,
        }
    }
}

/// A per-query predicate that produces a [`SegmentDocPredicate`] for each
/// segment.
///
/// Implementing this trait is all that's needed to make a type usable in a
/// [`DocPredicateQuery`].
pub trait DocPredicate: Send + Sync + 'static + std::fmt::Debug {
    /// The per-segment predicate produced by [`Self::doc_predicate`].
    type SegmentDocPredicate: SegmentDocPredicate;

    /// Builds the predicate used to evaluate documents of `segment_reader`.
    ///
    /// Called once per segment; segment-level setup (such as opening
    /// fast-field columns) belongs here rather than in
    /// [`SegmentDocPredicate::eval`].
    fn doc_predicate(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<ConstOrVariableSegmentPredicate<Self::SegmentDocPredicate>>;
}

/// The per-segment predicate produced by a [`DocPredicate`].
pub trait SegmentDocPredicate: Send + 'static {
    /// Returns whether `doc_id` matches the predicate.
    fn eval(&mut self, doc_id: DocId) -> bool;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use proptest::prelude::*;

    use super::*;
    use crate::collector::{Count, DocSetCollector};
    use crate::query::VecDocSet;

    pub(crate) fn create_index_for_test(num_docs: u32) -> crate::Index {
        let schema_builder = crate::schema::Schema::builder();
        let schema = schema_builder.build();
        let index = crate::Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        for _ in 0..num_docs {
            writer.add_document(doc!()).unwrap();
        }
        writer.commit().unwrap();
        index
    }

    fn even_doc_id_query() -> DocPredicateQuery {
        FunctionPredicate::from(|_segment_reader: &SegmentReader| {
            Ok(move |doc_id: DocId| doc_id.is_multiple_of(2))
        })
        .into()
    }

    #[test]
    fn test_doc_predicate_query_matches_expected_documents() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.search(&even_doc_id_query(), &Count).unwrap(), 2);
    }

    #[test]
    fn test_doc_predicate_query_explain() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let query = even_doc_id_query();
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(&searcher))
            .unwrap();
        let segment_reader = searcher.segment_reader(0);

        assert!(weight.explain(segment_reader, 0).is_ok());
        assert!(weight.explain(segment_reader, 1).is_err());
    }

    #[test]
    fn test_doc_predicate_query_scorer_danger_seeks_to_next_match() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let query = even_doc_id_query();
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(&searcher))
            .unwrap();
        let segment_reader = searcher.segment_reader(0);

        let (seek_result, mut scorer) = weight.scorer_danger(segment_reader, 1, 1.0).unwrap();
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(2));
        assert_eq!(scorer.seek_danger(2), SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);
    }

    #[test]
    fn test_doc_predicate_query_scorer_danger_target_is_a_match() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let query = even_doc_id_query();
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(&searcher))
            .unwrap();
        let segment_reader = searcher.segment_reader(0);

        let (seek_result, scorer) = weight.scorer_danger(segment_reader, 2, 1.0).unwrap();
        assert_eq!(seek_result, SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);
    }

    /// Matches even doc ids, and counts its evaluations.
    struct EvenDocIds {
        num_evals: Arc<AtomicUsize>,
    }

    impl SegmentDocPredicate for EvenDocIds {
        fn eval(&mut self, doc_id: DocId) -> bool {
            self.num_evals.fetch_add(1, AtomicOrdering::Relaxed);
            doc_id.is_multiple_of(2)
        }
    }

    /// `EvenDocIds`, with a fixed necessary condition.
    #[derive(Debug)]
    struct EvenWithNecessaryCondition {
        necessary_condition: Vec<DocId>,
        num_evals: Arc<AtomicUsize>,
    }

    impl EvenWithNecessaryCondition {
        fn new(necessary_condition: Vec<DocId>) -> Self {
            EvenWithNecessaryCondition {
                necessary_condition,
                num_evals: Arc::default(),
            }
        }
    }

    impl DocPredicate for EvenWithNecessaryCondition {
        type SegmentDocPredicate = EvenDocIds;

        fn doc_predicate(
            &self,
            _segment_reader: &SegmentReader,
        ) -> crate::Result<ConstOrVariableSegmentPredicate<EvenDocIds>> {
            Ok(ConstOrVariableSegmentPredicate::Variable {
                predicate: EvenDocIds {
                    num_evals: self.num_evals.clone(),
                },
                necessary_condition: Some(Box::new(VecDocSet::from(
                    self.necessary_condition.clone(),
                ))),
            })
        }
    }

    #[test]
    fn test_necessary_condition_restricts_evaluations() {
        let index = create_index_for_test(10);
        let searcher = index.reader().unwrap().searcher();
        let predicate = EvenWithNecessaryCondition::new(vec![1, 2, 3, 4, 6, 9]);
        let num_evals = predicate.num_evals.clone();
        let query: DocPredicateQuery = predicate.into();
        assert_eq!(searcher.search(&query, &DocSetCollector).unwrap().len(), 3);
        assert_eq!(num_evals.load(AtomicOrdering::Relaxed), 6);
    }

    #[test]
    fn test_necessary_condition_size_hint_and_cost() {
        let index = create_index_for_test(10);
        let searcher = index.reader().unwrap().searcher();
        let query: DocPredicateQuery =
            EvenWithNecessaryCondition::new(vec![1, 2, 3, 4, 6, 9]).into();
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(&searcher))
            .unwrap();
        let scorer = weight.scorer(searcher.segment_reader(0), 1.0).unwrap();
        assert_eq!(scorer.size_hint(), 6);
        assert_eq!(scorer.cost(), 6 * PREDICATE_EVAL_COST_FACTOR);
        // Without a necessary condition, all docs are candidates.
        let scorer = even_doc_id_query()
            .scorer(searcher.segment_reader(0), 1.0)
            .unwrap();
        assert_eq!(scorer.size_hint(), 10);
        assert_eq!(scorer.cost(), 10 * PREDICATE_EVAL_COST_FACTOR);
    }

    #[test]
    fn test_necessary_condition_scorer_danger() {
        let index = create_index_for_test(10);
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let scorer_danger = |necessary_condition: Vec<DocId>, target: DocId| {
            let predicate = EvenWithNecessaryCondition::new(necessary_condition);
            let num_evals = predicate.num_evals.clone();
            let query: DocPredicateQuery = predicate.into();
            let (seek_result, scorer) = query.scorer_danger(segment_reader, target, 1.0).unwrap();
            (seek_result, scorer, num_evals.load(AtomicOrdering::Relaxed))
        };

        // The necessary condition starts after the target.
        let (seek_result, mut scorer, num_evals) = scorer_danger(vec![4, 6], 1);
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(4));
        assert_eq!(num_evals, 0);
        assert_eq!(scorer.seek_danger(4), SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 4);

        // The target is the first candidate, and matches.
        let (seek_result, scorer, _) = scorer_danger(vec![2, 6], 2);
        assert_eq!(seek_result, SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);

        // The target is a candidate, but does not match.
        let (seek_result, mut scorer, num_evals) = scorer_danger(vec![1, 3, 4], 3);
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(4));
        assert_eq!(num_evals, 1);
        assert_eq!(scorer.seek_danger(4), SeekDangerResult::Found);

        // The target is not a candidate: it is not evaluated.
        let (seek_result, _, num_evals) = scorer_danger(vec![1, 6], 2);
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(6));
        assert_eq!(num_evals, 0);

        // No match after the target. The lower bound can stop on a non-matching candidate.
        let (seek_result, mut scorer, _) = scorer_danger(vec![1, 3], 2);
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(3));
        assert_eq!(scorer.seek_danger(3), SeekDangerResult::SeekLowerBound(4));
        assert_eq!(
            scorer.seek_danger(4),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
    }

    proptest! {
        #[test]
        fn proptest_necessary_condition_doc_set(
            candidates in prop::collection::btree_set(0u32..200, 0..60),
            modulo in 1u32..5,
            targets in prop::collection::vec(0u32..220, 0..30),
            advances in prop::collection::vec(any::<bool>(), 0..30),
        ) {
            let candidates: Vec<DocId> = candidates.into_iter().collect();
            let expected: Vec<DocId> = candidates
                .iter()
                .copied()
                .filter(|doc| doc.is_multiple_of(modulo))
                .collect();
            let new_doc_set = || {
                DocPredicateDocSet::new(
                    move |doc: DocId| doc.is_multiple_of(modulo),
                    Box::new(VecDocSet::from(candidates.clone())),
                )
            };
            let first_match = |target: DocId| {
                expected
                    .iter()
                    .copied()
                    .find(|doc| *doc >= target)
                    .unwrap_or(TERMINATED)
            };

            // advance
            let mut doc_set = new_doc_set();
            let mut matches: Vec<DocId> = Vec::new();
            while doc_set.doc() != TERMINATED {
                matches.push(doc_set.doc());
                doc_set.advance();
            }
            prop_assert_eq!(&matches, &expected);

            // interleaved seek and advance
            let mut doc_set = new_doc_set();
            for (target, advance) in targets.iter().zip(advances.iter()) {
                let target = (*target).max(doc_set.doc());
                if *advance && doc_set.doc() != TERMINATED {
                    let current = doc_set.doc();
                    prop_assert_eq!(doc_set.advance(), first_match(current + 1));
                } else {
                    prop_assert_eq!(doc_set.seek(target), first_match(target));
                }
            }

            // seek_danger, following its contract: strictly increasing targets, respecting
            // the returned lower bounds.
            let mut sorted_targets = targets.clone();
            sorted_targets.sort_unstable();
            sorted_targets.dedup();
            let mut doc_set = new_doc_set();
            let mut lower_bound = doc_set.doc();
            let mut previous_target = None;
            for requested_target in sorted_targets {
                let target = requested_target.max(lower_bound);
                if previous_target.is_some_and(|previous| previous >= target) {
                    continue;
                }
                previous_target = Some(target);
                let next_match = first_match(target);
                match doc_set.seek_danger(target) {
                    SeekDangerResult::Found => {
                        prop_assert_eq!(next_match, target);
                        prop_assert_eq!(doc_set.doc(), target);
                    }
                    SeekDangerResult::SeekLowerBound(bound) => {
                        prop_assert!(next_match != target || target == TERMINATED);
                        prop_assert!(bound > target || target == TERMINATED);
                        prop_assert!(bound <= next_match);
                        lower_bound = bound;
                    }
                }
            }
        }
    }

    #[test]
    fn test_doc_predicate_query_scorer_danger_target_past_max_doc() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let query = even_doc_id_query();
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(&searcher))
            .unwrap();
        let segment_reader = searcher.segment_reader(0);

        let (seek_result, _scorer) = weight.scorer_danger(segment_reader, 4, 1.0).unwrap();
        assert_eq!(seek_result, SeekDangerResult::SeekLowerBound(TERMINATED));
    }
}
