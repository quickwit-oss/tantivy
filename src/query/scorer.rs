use std::ops::DerefMut;

use downcast_rs::impl_downcast;

use crate::docset::DocSet;
use crate::{DocId, Score, TERMINATED};

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](crate::query::Query).
pub trait Scorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&mut self) -> Score;
}

impl_downcast!(Scorer);

impl Scorer for Box<dyn Scorer> {
    #[inline]
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }
}

pub trait PruningScorer: Scorer {
    fn set_threshold(&mut self, score: Score);
}

impl_downcast!(PruningScorer);

impl Scorer for Box<dyn PruningScorer> {
    #[inline]
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }
}

impl PruningScorer for Box<dyn PruningScorer> {
    #[inline]
    fn set_threshold(&mut self, score: Score) {
        self.deref_mut().set_threshold(score);
    }
}

pub struct BasicPruningScorer {
    scorer: Box<dyn Scorer>,
    threshold: Score,
    current: (DocId, Score),
}
impl BasicPruningScorer {
    pub fn new(scorer: Box<dyn Scorer>, threshold: Score) -> Self {
        let mut pruning = Self {
            scorer,
            threshold,
            current: (0, Score::MIN),
        };
        pruning.advance();
        pruning
    }
}
impl Scorer for BasicPruningScorer {
    #[inline]
    fn score(&mut self) -> Score {
        self.current.1
    }
}
impl PruningScorer for BasicPruningScorer {
    #[inline]
    fn set_threshold(&mut self, score: Score) {
        self.threshold = score;
    }
}
impl DocSet for BasicPruningScorer {
    #[inline]
    fn doc(&self) -> crate::DocId {
        self.current.0
    }

    fn advance(&mut self) -> crate::DocId {
        let mut doc_id = self.scorer.doc();
        while doc_id != TERMINATED {
            let score = self.scorer.score();
            if score > self.threshold {
                self.current = (doc_id, score);
                self.scorer.advance();
                return doc_id;
            }
            doc_id = self.scorer.advance();
        }
        self.current = (TERMINATED, Score::MIN);
        TERMINATED
    }

    /// The number of elements yielded by a PruningScorer depends on the threshold and cannot be
    /// computed ahead of time, so just defer to the inner scorer.
    fn size_hint(&self) -> u32 {
        self.scorer.size_hint()
    }
}
