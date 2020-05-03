use crate::postings::Postings;
use crate::DocId;

/// Inverted list with additional information about the maximum term frequency
/// within a block, as well as globally within the list.
pub trait BlockMaxPostings: Postings {
    /// Returns the maximum frequency in the entire list.
    fn max_term_freq(&self) -> u32;
    /// Returns the maximum frequency in the current block.
    fn block_max_term_freq(&mut self) -> u32;
    /// Returns the document with the largest frequency.
    fn max_doc(&self) -> DocId;
    /// Returns the document with the largest frequency within the current
    /// block.
    fn block_max_doc(&self) -> DocId;
}
