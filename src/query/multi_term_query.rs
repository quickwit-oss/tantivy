use schema::Term;
use query::Query;
use common::TimerTree;
use common::OpenTimer;
use std::io;
use core::searcher::Searcher;
use collector::Collector;
use core::searcher::SegmentLocalId;
use core::SegmentReader;
use postings::Postings;
use postings::SegmentPostings;
use postings::UnionPostings;

pub struct MultiTermQuery {
    terms: Vec<Term>,    
}

impl Query for MultiTermQuery {
    
    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> io::Result<TimerTree> {
        let mut timer_tree = TimerTree::new();
        {
            let mut search_timer = timer_tree.open("search");
            for (segment_ord, segment_reader) in searcher.segments().iter().enumerate() {
                let mut segment_search_timer = search_timer.open("segment_search");
                {
                    let _ = segment_search_timer.open("set_segment");
                    try!(collector.set_segment(segment_ord as SegmentLocalId, &segment_reader));
                }
                let mut postings = self.search_segment(segment_reader, segment_search_timer.open("get_postings"));
                
                {
                    let _collection_timer = segment_search_timer.open("collection");
                    let mut score: f32 = 0.0;
                    while postings.next() {
                        for &ord in postings.active_posting_ordinals() {
                            let term_freq = postings[ord].freq();
                            score += (term_freq as f32); // * idf
                        }
                        // collector.collect(postings.doc(), score);
                    }
                }
            }
        }
        Ok(timer_tree)
    }
}

impl MultiTermQuery {
    
    pub fn new(terms: Vec<Term>) -> MultiTermQuery {
        MultiTermQuery {
            terms: terms,
        }
    }
        
    fn search_segment<'a, 'b>(&'b self, reader: &'b SegmentReader, mut timer: OpenTimer<'a>) -> UnionPostings<SegmentPostings> {
        let mut segment_postings: Vec<SegmentPostings> = Vec::new();
        {
            let mut decode_timer = timer.open("decode_all");
            for term in self.terms.iter() {
                let _decode_one_timer = decode_timer.open("decode_one");
                match reader.read_postings(term) {
                    Some(postings) => {
                        segment_postings.push(postings);
                    }
                    None => {
                        // currently this is a strict intersection.
                        segment_postings.push(SegmentPostings::empty());
                    }
                }
            }
        }
        UnionPostings::from(segment_postings)
    }
}
