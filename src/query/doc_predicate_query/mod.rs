use std::sync::Arc;

mod function_predicate;

pub use function_predicate::FunctionPredicate;

use crate::docset::{SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::{ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
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

/// A [`DocSet`] that walks documents by repeatedly evaluating a
/// [`SegmentDocPredicate`], starting from doc `0`.
pub struct DocPredicateDocSet<TSegmentDocPredicate> {
    doc_predicate: TSegmentDocPredicate,
    doc: DocId,
    max_doc: DocId,
}

impl<TSegmentDocPredicate: SegmentDocPredicate> DocSet
    for DocPredicateDocSet<TSegmentDocPredicate>
{
    fn advance(&mut self) -> DocId {
        if self.doc == TERMINATED {
            return TERMINATED;
        }
        self.find_match(self.doc + 1)
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc);
        if self.doc == TERMINATED {
            return TERMINATED;
        }
        self.find_match(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target >= self.max_doc {
            self.doc = TERMINATED;
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }
        if self.doc_predicate.eval(target) {
            self.doc = target;
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl<TSegmentDocPredicate: SegmentDocPredicate> DocPredicateDocSet<TSegmentDocPredicate> {
    fn find_match(&mut self, mut target: DocId) -> DocId {
        loop {
            match self.seek_danger(target) {
                SeekDangerResult::Found => return target,
                SeekDangerResult::SeekLowerBound(next_target) => {
                    if next_target >= TERMINATED {
                        return TERMINATED;
                    }
                    target = next_target;
                }
            }
        }
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
        let doc_predicate = self.doc_predicate(segment_reader)?;
        let mut doc_set = DocPredicateDocSet {
            doc_predicate,
            doc: 0u32,
            max_doc: segment_reader.max_doc(),
        };
        doc_set.doc = doc_set.find_match(0);
        Ok(Box::new(ConstScorer::new(doc_set, boost)) as Box<dyn Scorer>)
    }

    fn scorer_danger(
        &self,
        segment_reader: &SegmentReader,
        target: DocId,
        boost: f32,
    ) -> crate::Result<(SeekDangerResult, Box<dyn Scorer>)> {
        let doc_predicate = self.doc_predicate(segment_reader)?;
        let mut doc_set = DocPredicateDocSet {
            doc_predicate,
            doc: target,
            max_doc: segment_reader.max_doc(),
        };
        let seek_result = doc_set.seek_danger(target);
        let scorer = Box::new(ConstScorer::new(doc_set, boost)) as Box<dyn Scorer>;
        Ok((seek_result, scorer))
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
    ) -> crate::Result<Self::SegmentDocPredicate>;
}

/// The per-segment predicate produced by a [`DocPredicate`].
pub trait SegmentDocPredicate: Send + 'static {
    /// Returns whether `doc_id` matches the predicate.
    fn eval(&mut self, doc_id: DocId) -> bool;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::collector::Count;

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
            Ok(move |doc_id: DocId| doc_id % 2 == 0)
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
