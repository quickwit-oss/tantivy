use super::Weight;
use crate::core::searcher::Searcher;
use crate::query::Explanation;
use crate::DocAddress;
use crate::Term;
use downcast_rs::impl_downcast;
use std::collections::BTreeSet;
use std::fmt;

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
pub trait Query: QueryClone + Send + Sync + downcast_rs::Downcast + fmt::Debug {
    /// Create the weight associated to a query.
    ///
    /// If scoring is not required, setting `scoring_enabled` to `false`
    /// can increase performances.
    ///
    /// See [`Weight`](./trait.Weight.html).
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> crate::Result<Box<dyn Weight>>;

    /// Returns an `Explanation` for the score of the document.
    fn explain(&self, searcher: &Searcher, doc_address: DocAddress) -> crate::Result<Explanation> {
        let reader = searcher.segment_reader(doc_address.segment_ord);
        let weight = self.weight(searcher, true)?;
        weight.explain(reader, doc_address.doc_id)
    }

    /// Returns the number of documents matching the query.
    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        let weight = self.weight(searcher, false)?;
        let mut result = 0;
        for reader in searcher.segment_readers() {
            result += weight.count(reader)? as usize;
        }
        Ok(result)
    }

    /// Extract all of the terms associated to the query and insert them in the
    /// term set given in arguments.
    fn query_terms(&self, _term_set: &mut BTreeSet<Term>) {}
}

/// Implements `box_clone`.
pub trait QueryClone {
    /// Returns a boxed clone of `self`.
    fn box_clone(&self) -> Box<dyn Query>;
}

impl<T> QueryClone for T
where
    T: 'static + Query + Clone,
{
    fn box_clone(&self) -> Box<dyn Query> {
        Box::new(self.clone())
    }
}

impl Query for Box<dyn Query> {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> crate::Result<Box<dyn Weight>> {
        self.as_ref().weight(searcher, scoring_enabled)
    }

    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        self.as_ref().count(searcher)
    }

    fn query_terms(&self, term_set: &mut BTreeSet<Term<Vec<u8>>>) {
        self.as_ref().query_terms(term_set);
    }
}

impl QueryClone for Box<dyn Query> {
    fn box_clone(&self) -> Box<dyn Query> {
        self.as_ref().box_clone()
    }
}

impl_downcast!(Query);
