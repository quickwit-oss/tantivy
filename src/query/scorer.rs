use common::BitSet;
use docset::{DocSet, SkipResult};
use downcast_rs;
use std::ops::DerefMut;
use DocId;
use Score;

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](./trait.Query.html).
pub trait Scorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&mut self) -> Score;

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each(&mut self, callback: &mut FnMut(DocId, Score)) {
        while self.advance() {
            callback(self.doc(), self.score());
        }
    }
}

impl_downcast!(Scorer);


impl Scorer for Box<Scorer> {
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }

    fn for_each(&mut self, callback: &mut FnMut(DocId, Score)) {
        let scorer = self.deref_mut();
        scorer.for_each(callback);
    }
}

/// Wraps a `DocSet` and simply returns a constant `Scorer`.
/// The `ConstScorer` is useful if you have a `DocSet` where
/// you needed a scorer.
///
/// The `ConstScorer`'s constant score can be set
/// by calling `.set_score(...)`.
pub struct ConstScorer<TDocSet: DocSet> {
    docset: TDocSet,
    score: Score,
}

impl<TDocSet: DocSet> ConstScorer<TDocSet> {
    /// Creates a new `ConstScorer`.
    pub fn new(docset: TDocSet) -> ConstScorer<TDocSet> {
        ConstScorer {
            docset,
            score: 1f32,
        }
    }

    /// Sets the constant score to a different value.
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

    fn append_to_bitset(&mut self, bitset: &mut BitSet) {
        self.docset.append_to_bitset(bitset);
    }
}

impl<TDocSet: DocSet + 'static> Scorer for ConstScorer<TDocSet> {
    fn score(&mut self) -> Score {
        1f32
    }
}
