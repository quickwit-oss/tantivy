use Result;
use core::SegmentReader;
use schema::Document;
use collector::Collector;
use common::TimerTree;
use query::Query;
use DocId;
use DocAddress;
use schema::Term;

#[derive(Debug)]
pub struct Searcher {
    segment_readers: Vec<SegmentReader>,
}

impl Searcher {

    pub fn doc(&self, doc_address: &DocAddress) -> Result<Document> {
        // TODO err
        let DocAddress(segment_local_id, doc_id) = *doc_address;
        let segment_reader = &self.segment_readers[segment_local_id as usize];
        segment_reader.doc(doc_id)
    }
    
    pub fn num_docs(&self,) -> DocId {
        self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.num_docs())
            .fold(0u32, |acc, val| acc + val)
    }

    pub fn doc_freq(&self, term: &Term) -> u32 {
        self.segment_readers
            .iter()
            .map(|segment_reader| segment_reader.doc_freq(term))
            .fold(0u32, |acc, val| acc + val)
    }
    
    pub fn segment_readers(&self,) -> &Vec<SegmentReader> {
        &self.segment_readers
    }

    pub fn segment_reader(&self, segment_ord: usize) -> &SegmentReader {
        &self.segment_readers[segment_ord]
    }

    pub fn from_readers(segment_readers: Vec<SegmentReader>) -> Searcher {
        Searcher {
            segment_readers: segment_readers,
        }
    }
    
    pub fn search<Q: Query, C: Collector>(&self, query: &Q, collector: &mut C) -> Result<TimerTree> {
        query.search(self, collector)
    }
}
