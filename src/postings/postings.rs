use docset::DocSet;

/// Postings (also called inverted list)
///
/// For a given term, it is the list of doc ids of the doc
/// containing the term. Optionally, for each document,
/// it may also give access to the term frequency
/// as well as the list of term positions.
///
/// Its main implementation is `SegmentPostings`,
/// but other implementations mocking `SegmentPostings` exist,
/// for merging segments or for testing.
pub trait Postings: DocSet + 'static {
    /// Returns the term frequency
    fn term_freq(&self) -> u32;

    /// Returns the list of positions of the term, expressed as a list of
    /// token ordinals.
    fn positions_with_offset(&self, offset: u32, output: &mut Vec<u32>);
}
