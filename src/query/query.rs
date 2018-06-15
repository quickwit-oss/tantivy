use super::Weight;
use collector::Collector;
use core::searcher::Searcher;
use downcast;
use std::fmt;
use Result;
use SegmentLocalId;

/// The `Query` trait defines a set of documents and a scoring method
/// for those documents.
///
/// The `Query` trait is in charge of defining :
///
/// - a set of documents
/// - a way to score these documents
///
/// When performing a [search](#method.search),  these documents will then
/// be pushed to a [Collector](../collector/trait.Collector.html),
/// which will in turn be in charge of deciding what to do with them.
///
/// Concretely, this scored docset is represented by the
/// [`Scorer`](./trait.Scorer.html) trait.
///
/// Because our index is actually split into segments, the
/// query does not actually directly creates `DocSet` object.
/// Instead, the query creates a [`Weight`](./trait.Weight.html)
/// object for a given searcher.
///
/// The weight object, in turn, makes it possible to create
/// a scorer for a specific [`SegmentReader`](../struct.SegmentReader.html).
///
/// So to sum it up :
/// - a `Query` is recipe to define a set of documents as well the way to score them.
/// - a `Weight` is this recipe tied to a specific `Searcher`. It may for instance
/// hold statistics about the different term of the query. It is created by the query.
/// - a `Scorer` is a cursor over the set of matching documents, for a specific
/// [`SegmentReader`](../struct.SegmentReader.html). It is created by the
/// [`Weight`](./trait.Weight.html).
///
/// When implementing a new type of `Query`, it is normal to implement a
/// dedicated `Query`, `Weight` and `Scorer`.
pub trait Query: QueryClone + downcast::Any + fmt::Debug {
    /// Create the weight associated to a query.
    ///
    /// If scoring is not required, setting `scoring_enabled` to `false`
    /// can increase performances.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>>;

    /// Returns the number of documents matching the query.
    fn count(&self, searcher: &Searcher) -> Result<usize> {
        let weight = self.weight(searcher, false)?;
        let mut result = 0;
        for reader in searcher.segment_readers() {
            result += weight.count(reader)? as usize;
        }
        Ok(result)
    }

    /// Search works as follows :
    ///
    /// First the weight object associated to the query is created.
    ///
    /// Then, the query loops over the segments and for each segment :
    /// - setup the collector and informs it that the segment being processed has changed.
    /// - creates a `Scorer` object associated for this segment
    /// - iterate throw the matched documents and push them to the collector.
    ///
    fn search(&self, searcher: &Searcher, collector: &mut Collector) -> Result<()> {
        let scoring_enabled = collector.requires_scoring();
        let weight = self.weight(searcher, scoring_enabled)?;
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            collector.set_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            scorer.collect(collector, segment_reader.delete_bitset());
        }
        Ok(())
    }
}

pub trait QueryClone {
    fn box_clone(&self) -> Box<Query>;
}

impl<T> QueryClone for T
where
    T: 'static + Query + Clone,
{
    fn box_clone(&self) -> Box<Query> {
        Box::new(self.clone())
    }
}

#[allow(missing_docs)]
mod downcast_impl {
    downcast!(super::Query);
}
