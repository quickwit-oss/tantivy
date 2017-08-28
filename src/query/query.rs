use Result;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use SegmentLocalId;
use super::Weight;
use std::fmt;
use std::any::Any;


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
pub trait Query: fmt::Debug {
    /// Used to make it possible to cast Box<Query>
    /// into a specific type. This is mostly useful for unit tests.
    fn as_any(&self) -> &Any;

    /// Create the weight associated to a query.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>>;

    /// Search works as follows :
    ///
    /// First the weight object associated to the query is created.
    ///
    /// Then, the query loops over the segments and for each segment :
    /// - setup the collector and informs it that the segment being processed has changed.
    /// - creates a `Scorer` object associated for this segment
    /// - iterate throw the matched documents and push them to the collector.
    ///
    fn search(&self, searcher: &Searcher, collector: &mut Collector) -> Result<TimerTree> {
        let mut timer_tree = TimerTree::default();
        let weight = try!(self.weight(searcher));
        {
            let mut search_timer = timer_tree.open("search");
            for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let mut segment_search_timer = search_timer.open("segment_search");
                {
                    let _ = segment_search_timer.open("set_segment");
                    try!(collector.set_segment(
                        segment_ord as SegmentLocalId,
                        segment_reader,
                    ));
                }
                let mut scorer = try!(weight.scorer(segment_reader));
                {
                    let _collection_timer = segment_search_timer.open("collection");
                    scorer.collect(collector);
                }
            }
        }
        Ok(timer_tree)
    }
}
