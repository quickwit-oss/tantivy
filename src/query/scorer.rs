use std::mem::{transmute_copy, ManuallyDrop};
use std::ops::DerefMut;

use downcast_rs::impl_downcast;

use crate::docset::DocSet;
use crate::query::Explanation;
use crate::{DocId, Score, TERMINATED};

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](crate::query::Query).
pub trait Scorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&mut self) -> Score;

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
        &mut self,
        threshold: Score,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) {
        for_each_pruning_scorer_default_impl(self, threshold, callback);
    }

    fn explain(&mut self) -> Explanation {
        let score = self.score();
        let name = std::any::type_name_of_val(self);
        Explanation::new(name, score)
    }
}

/// Boxes a scorer. Prefer this to Box::new as it avoids double boxing
/// when TScorer is already a Box<dyn Scorer>.
pub fn box_scorer<TScorer: Scorer>(scorer: TScorer) -> Box<dyn Scorer> {
    if std::any::TypeId::of::<TScorer>() == std::any::TypeId::of::<Box<dyn Scorer>>() {
        unsafe {
            let forget_me = ManuallyDrop::new(scorer);
            transmute_copy::<TScorer, Box<dyn Scorer>>(&forget_me)
        }
    } else {
        Box::new(scorer)
    }
}

impl_downcast!(Scorer);

impl Scorer for Box<dyn Scorer> {
    #[inline]
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }

    fn for_each_pruning(
        &mut self,
        threshold: Score,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) {
        self.deref_mut().for_each_pruning(threshold, callback);
    }
}

/// Calls `callback` with all of the `(doc, score)` for which score
/// is exceeding a given threshold.
///
/// This method is useful for the [`TopDocs`](crate::collector::TopDocs) collector.
/// For all docsets, the blanket implementation has the benefit
/// of prefiltering (doc, score) pairs, avoiding the
/// virtual dispatch cost.
///
/// More importantly, it makes it possible for scorers to implement
/// important optimization (e.g. BlockWAND for union).
pub(crate) fn for_each_pruning_scorer_default_impl<TScorer: Scorer + ?Sized>(
    scorer: &mut TScorer,
    mut threshold: Score,
    callback: &mut dyn FnMut(DocId, Score) -> Score,
) {
    let mut doc = scorer.doc();
    while doc != TERMINATED {
        let score = scorer.score();
        if score > threshold {
            threshold = callback(doc, score);
        }
        doc = scorer.advance();
    }
}
