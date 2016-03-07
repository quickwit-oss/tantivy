use core::reader::SegmentReader;
use core::index::Index;
use core::index::Segment;
use core::index::SegmentId;
use core::schema::DocId;
use core::schema::Document;
use core::collector::Collector;
use std::collections::HashMap;
use std::io;
use core::schema::Term;


pub struct Searcher {
    segments: Vec<SegmentReader>,
    segments_idx: HashMap<SegmentId, usize>,
}

#[derive(Debug)]
pub struct DocAddress(pub SegmentId, pub DocId);

impl Searcher {

    pub fn doc(&self, doc_address: &DocAddress) -> io::Result<Document> {
        // TODO err
        let DocAddress(ref segment_id, ref doc_id) = *doc_address;
        let segment_ord = self.segments_idx.get(&segment_id).unwrap();
        let segment_reader = &self.segments[segment_ord.clone()];
        segment_reader.doc(doc_id)
    }

    fn add_segment(&mut self, segment: Segment) -> io::Result<()> {
        SegmentReader::open(segment.clone())
            .map(|segment_reader| {
                let segment_ord = self.segments.len();
                self.segments.push(segment_reader);
                self.segments_idx.insert(segment.id(), segment_ord);
            })
    }

    fn new() -> Searcher {
        Searcher {
            segments: Vec::new(),
            segments_idx: HashMap::new(),
        }
    }

    pub fn for_index(index: Index) -> io::Result<Searcher> {
        let mut searcher = Searcher::new();
        for segment in index.segments().into_iter() {
            try!(searcher.add_segment(segment));
        }
        Ok(searcher)
    }

    pub fn search(&self, terms: &Vec<Term>, collector: &mut Collector) {
        for segment in &self.segments {
            collector.set_segment(&segment);
            let postings = segment.search(terms);
            for doc_id in postings {
                collector.collect(doc_id);
            }
        }
    }

}
