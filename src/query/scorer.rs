use DocSet;
use DocId;
use Score;
use collector::Collector;
use postings::SkipResult;
use common::DocBitSet;
use std::ops::{Deref, DerefMut};

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](./trait.Query.html).
pub trait Scorer: DocSet {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&self) -> Score;

    /// Consumes the complete `DocSet` and
    /// push the scored documents to the collector.
    fn collect(&mut self, collector: &mut Collector) {
        while self.advance() {
            collector.collect(self.doc(), self.score());
        }
    }
}

impl<'a> Scorer for Box<Scorer + 'a> {
    fn score(&self) -> Score {
        self.deref().score()
    }

    fn collect(&mut self, collector: &mut Collector) {
        let scorer = self.deref_mut();
        while scorer.advance() {
            collector.collect(scorer.doc(), scorer.score());
        }
    }
}

/// `EmptyScorer` is a dummy `Scorer` in which no document matches.
///
/// It is useful for tests and handling edge cases.
pub struct EmptyScorer;

impl DocSet for EmptyScorer {
    fn advance(&mut self) -> bool {
        false
    }

    fn doc(&self) -> DocId {
        DocId::max_value()
    }

    fn size_hint(&self) -> u32 {
        0
    }
}

impl Scorer for EmptyScorer {
    fn score(&self) -> Score {
        0f32
    }
}

pub struct ConstScorer<TDocSet: DocSet> {
    docset: TDocSet,
    score: Score,
}

impl<TDocSet: DocSet> ConstScorer<TDocSet> {
    pub fn new(docset: TDocSet) -> ConstScorer<TDocSet> {
        ConstScorer {
            docset,
            score: 1f32,
        }
    }

    pub fn set_score(&mut self, score: Score) {
        self.score = score;
    }
}

impl<TDocSet: DocSet> DocSet for ConstScorer<TDocSet> {
    fn advance(&mut self) -> bool {
        self.docset.advance()
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        self.docset.skip_next(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId]) -> usize {
        self.docset.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        self.docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.docset.size_hint()
    }

    fn to_doc_bitset(&mut self, max_doc: DocId) -> DocBitSet {
        self.docset.to_doc_bitset(max_doc)
    }
}

impl<TDocSet: DocSet> Scorer for ConstScorer<TDocSet> {
    fn score(&self) -> Score {
        1f32
    }
}
