use DocSet;
use DocId;
use Score;
use collector::Collector;
use std::ops::{Deref, DerefMut};

/// Scored set of documents matching a query within a specific segment.
///
/// See [Query](./trait.Query.html).
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

/// EmptyScorer is a dummy Scorer in which no document matches.
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
}

impl Scorer for EmptyScorer {
    fn score(&self) -> Score {
        0f32
    }
}
