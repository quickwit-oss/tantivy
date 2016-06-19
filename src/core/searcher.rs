use core::SegmentReader;
use core::index::Index;
use core::index::Segment;
use DocId;
use schema::Document;
use collector::Collector;
use std::io;
use common::TimerTree;
use postings::Postings;
use query::Query;

#[derive(Debug)]
pub struct Searcher {
    segments: Vec<SegmentReader>,
}

/// A segment local id identifies a segment.
/// It only makes sense for a given searcher.
pub type SegmentLocalId = u32;

#[derive(Debug)]
pub struct DocAddress(pub SegmentLocalId, pub DocId);

impl Searcher {

    pub fn doc(&self, doc_address: &DocAddress) -> io::Result<Document> {
        // TODO err
        let DocAddress(ref segment_local_id, ref doc_id) = *doc_address;
        let segment_reader = &self.segments[*segment_local_id as usize];
        segment_reader.doc(doc_id)
    }

    fn add_segment(&mut self, segment: Segment) -> io::Result<()> {
        let segment_reader = try!(SegmentReader::open(segment.clone()));
        self.segments.push(segment_reader);
        Ok(())
    }

    fn new() -> Searcher {
        Searcher {
            segments: Vec::new(),
        }
    }
    
    pub fn segments(&self,) -> &Vec<SegmentReader> {
        &self.segments
    }

    pub fn for_index(index: Index) -> io::Result<Searcher> {
        let mut searcher = Searcher::new();
        for segment in index.segments().into_iter() {
            try!(searcher.add_segment(segment));
        }
        Ok(searcher)
    }
    
    pub fn search<Q: Query, C: Collector>(&self, query: &Q, collector: &mut C) -> io::Result<TimerTree> {
        query.search(self, collector)
    }
    
    // pub fn search<C: Collector>(&self, terms: &Vec<Term>, collector: &mut C) -> io::Result<TimerTree> {
    //     let mut timer_tree = TimerTree::new();
    //     {
    //         let mut search_timer = timer_tree.open("search");
    //         for (segment_ord, segment) in self.segments.iter().enumerate() {
    //             let mut segment_search_timer = search_timer.open("segment_search");
    //             {
    //                 let _ = segment_search_timer.open("set_segment");
    //                 try!(collector.set_segment(segment_ord as SegmentLocalId, &segment));
    //             }
    //             let mut postings = segment.search(terms, segment_search_timer.open("get_postings"));
    //             {
    //                 let _collection_timer = segment_search_timer.open("collection");
    //                 while postings.next() {
    //                     collector.collect(postings.doc());
    //                 }
    //             }
    //         }
    //     }
    //     Ok(timer_tree)
    // }

}
