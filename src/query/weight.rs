use super::Scorer;
use Result;
use core::SegmentReader;


/// A Weight is the specialization of a Query
/// for a given set of segments.
///
/// See [Query](./trait.Query.html).
pub trait Weight {
    /// Returns the scorer for the given segment.
    /// See [Query](./trait.Query.html).
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>>;
}
