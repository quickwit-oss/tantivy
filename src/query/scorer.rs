use std::ops::DerefMut;

use downcast_rs::impl_downcast;

use crate::docset::{DocSet, COLLECT_BLOCK_BUFFER_LEN};
use crate::{DocId, Score};

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](crate::query::Query).
pub trait Scorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&mut self) -> Score;

    /// Returns true if [`Scorer::score_doc`] can score arbitrary buffered docs without
    /// repositioning the scorer.
    fn can_score_doc(&self) -> bool {
        false
    }

    /// Returns the score for `doc` with its term frequency.
    fn score_doc(&mut self, _doc: DocId, _term_freq: u32) -> Score {
        panic!("score_doc is not supported by this scorer. You need check can_score_doc() before calling this method.")
    }

    /// Fills docs and term frequencies up to `horizon`.
    fn fill_buffer_up_to_with_term_freqs(
        &mut self,
        horizon: DocId,
        docs: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN],
        term_freqs: &mut [u32; COLLECT_BLOCK_BUFFER_LEN],
    ) -> usize {
        let len = DocSet::fill_buffer_up_to(self, horizon, docs);
        term_freqs[..len].fill(1);
        len
    }
}

impl_downcast!(Scorer);

impl Scorer for Box<dyn Scorer> {
    #[inline]
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }

    #[inline]
    fn can_score_doc(&self) -> bool {
        self.as_ref().can_score_doc()
    }

    #[inline]
    fn score_doc(&mut self, doc: DocId, term_freq: u32) -> Score {
        self.deref_mut().score_doc(doc, term_freq)
    }

    #[inline]
    fn fill_buffer_up_to_with_term_freqs(
        &mut self,
        horizon: DocId,
        docs: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN],
        term_freqs: &mut [u32; COLLECT_BLOCK_BUFFER_LEN],
    ) -> usize {
        self.deref_mut()
            .fill_buffer_up_to_with_term_freqs(horizon, docs, term_freqs)
    }
}
