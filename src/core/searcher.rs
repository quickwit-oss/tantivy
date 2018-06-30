use collector::Collector;
use core::InvertedIndexReader;
use core::SegmentReader;
use query::Query;
use schema::Document;
use schema::Schema;
use schema::{Field, Term};
use std::fmt;
use std::sync::Arc;
use termdict::TermMerger;
use DocAddress;
use Result;

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
///
pub struct Searcher {
    schema: Schema,
    segment_readers: Vec<SegmentReader>,
}

impl Searcher {
    /// Creates a new `Searcher`
    pub(crate) fn new(schema: Schema, segment_readers: Vec<SegmentReader>) -> Searcher {
        Searcher {
            schema,
            segment_readers,
        }
    }
    /// Fetches a document from tantivy's store given a `DocAddress`.
    ///
    /// The searcher uses the segment ordinal to route the
    /// the request to the right `Segment`.
    pub fn doc(&self, doc_address: &DocAddress) -> Result<Document> {
        let DocAddress(segment_local_id, doc_id) = *doc_address;
        let segment_reader = &self.segment_readers[segment_local_id as usize];
        segment_reader.doc(doc_id)
    }

    /// Access the schema associated to the index of this searcher.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.num_docs() as u64)
            .sum::<u64>()
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> u64 {
        self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.inverted_index(term.field()).doc_freq(term) as u64)
            .sum::<u64>()
    }

    /// Return the list of segment readers
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.segment_readers
    }

    /// Returns the segment_reader associated with the given segment_ordinal
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        &self.segment_readers[segment_ord as usize]
    }

    /// Runs a query on the segment readers wrapped by the searcher
    pub fn search<C: Collector>(&self, query: &Query, collector: &mut C) -> Result<()> {
        query.search(self, collector)
    }

    /// Return the field searcher associated to a `Field`.
    pub fn field(&self, field: Field) -> FieldSearcher {
        let inv_index_readers = self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.inverted_index(field))
            .collect::<Vec<_>>();
        FieldSearcher::new(inv_index_readers)
    }
}

pub struct FieldSearcher {
    inv_index_readers: Vec<Arc<InvertedIndexReader>>,
}

impl FieldSearcher {
    fn new(inv_index_readers: Vec<Arc<InvertedIndexReader>>) -> FieldSearcher {
        FieldSearcher { inv_index_readers }
    }

    /// Returns a Stream over all of the sorted unique terms of
    /// for the given field.
    pub fn terms(&self) -> TermMerger {
        let term_streamers: Vec<_> = self.inv_index_readers
            .iter()
            .map(|inverted_index| inverted_index.terms().stream())
            .collect();
        TermMerger::new(term_streamers)
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let segment_ids = self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.segment_id())
            .collect::<Vec<_>>();
        write!(f, "Searcher({:?})", segment_ids)
    }
}
