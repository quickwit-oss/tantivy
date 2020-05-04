use crate::docset::DocSet;
use crate::DocId;
use crate::Score;
use downcast_rs::impl_downcast;
use std::ops::Deref;
use std::ops::DerefMut;

/// A set of documents matching a query within a specific segment
/// and having a maximum score within certain blocks.
///
/// See [`Query`](./trait.Query.html) and [`Scorer`](./trait.Scorer.html).
pub trait BlockMaxScorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the maximum score within the current block.
    ///
    /// The blocks are defined when indexing. For example, blocks can be
    /// have a specific number postings each, or can be optimized for
    /// retrieval speed. Read more in
    /// [Faster BlockMax WAND with Variable-sized Blocks][vbmw]
    ///
    /// This method will perform a bit of computation and is not cached.
    ///
    /// [vbmw]: https://dl.acm.org/doi/abs/10.1145/3077136.3080780
    fn block_max_score(&mut self) -> Score;

    /// Returns the last document in the current block.
    fn block_max_doc(&mut self) -> DocId;

    /// Returns the maximum possible score within the entire document set.
    fn max_score(&self) -> Score;
}

impl_downcast!(BlockMaxScorer);

impl BlockMaxScorer for Box<dyn BlockMaxScorer> {
    fn block_max_score(&mut self) -> Score {
        self.deref_mut().block_max_score()
    }
    fn block_max_doc(&mut self) -> DocId {
        self.deref_mut().block_max_doc()
    }
    fn max_score(&self) -> Score {
        self.deref().max_score()
    }
}
