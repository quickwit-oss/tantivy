use std::sync::Arc;

#[cfg(feature = "jitexpr")]
pub mod jitexpr_predicate;

use crate::docset::{SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::{ConstScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score};

/// A query that evaluates a boolean JIT expression against fast-field columns.
///
/// Variable names in the expression are resolved as fast-field names for each
/// segment. Multivalued columns contribute their first value. A document with a
/// missing input value does not match.
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

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut docset = self.predicate.scorer(reader, 1.0f32)?;
        if docset.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("CalculatedPredicateQuery", 1.0))
    }
}

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
                    if next_target >= self.max_doc {
                        self.doc = TERMINATED;
                        return TERMINATED;
                    }
                    target = next_target;
                }
            }
        }
    }
}
pub trait DocPredicateBoxable: std::fmt::Debug + 'static + Send + Sync {
    fn scorer(&self, segment_reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>>;
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
}

pub trait DocPredicate: Send + Sync + 'static + std::fmt::Debug {
    type SegmentDocPredicate: SegmentDocPredicate;

    fn doc_predicate(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::SegmentDocPredicate>;
}

pub trait SegmentDocPredicate: Send + Sync + 'static {
    fn eval(&self, doc_id: DocId) -> bool;
}
