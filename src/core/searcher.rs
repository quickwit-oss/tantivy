use Result;
use core::SegmentReader;
use schema::Document;
use collector::Collector;
use common::TimerTree;
use query::Query;
use DocId;
use DocAddress;
use schema::{Term, Field};
use termdict::{TermMerger, TermDictionary};
use std::sync::Arc;
use std::fmt;
use core::FieldReader;


/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
///
pub struct Searcher {
    segment_readers: Vec<SegmentReader>,
}

impl Searcher {
    /// Fetches a document from tantivy's store given a `DocAddress`.
    ///
    /// The searcher uses the segment ordinal to route the
    /// the request to the right `Segment`.
    pub fn doc(&self, doc_address: &DocAddress) -> Result<Document> {
        let DocAddress(segment_local_id, doc_id) = *doc_address;
        let segment_reader = &self.segment_readers[segment_local_id as usize];
        segment_reader.doc(doc_id)
    }

    /// Returns the overall number of documents in the index.
    pub fn num_docs(&self) -> DocId {
        self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.num_docs())
            .fold(0u32, |acc, val| acc + val)
    }

    /// Return the overall number of documents containing
    /// the given term.
    pub fn doc_freq(&self, term: &Term) -> u32 {
        self.segment_readers
            .iter()
            .map(|segment_reader| {
                segment_reader
                    .field_reader(term.field())
                    .unwrap() // TODO error handling
                    .doc_freq(term)
            })
            .fold(0u32, |acc, val| acc + val)
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
    pub fn search<C: Collector>(&self, query: &Query, collector: &mut C) -> Result<TimerTree> {
        query.search(self, collector)
    }


    // This API may change in the future.
    pub fn field(&self, field: Field) -> Result<FieldSearcher> {
        let field_readers = self.segment_readers
            .iter()
            .map(|segment_reader| {
                segment_reader.field_reader(field)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(FieldSearcher::new(field_readers))
    }
}




pub struct FieldSearcher {
    field_readers: Vec<Arc<FieldReader>>,
}


impl FieldSearcher {

    fn new(field_readers: Vec<Arc<FieldReader>>) -> FieldSearcher {
        FieldSearcher {
            field_readers: field_readers,
        }
    }


    /// Returns a Stream over all of the sorted unique terms of
    /// for the given field.
    pub fn terms(&self) -> TermMerger {
        let term_streamers: Vec<_> = self.field_readers
            .iter()
            .map(|field_reader| {
                field_reader.terms().stream()
            })
            .collect();
        TermMerger::new(term_streamers)
    }
}

impl From<Vec<SegmentReader>> for Searcher {
    fn from(segment_readers: Vec<SegmentReader>) -> Searcher {
        Searcher { segment_readers: segment_readers }
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
