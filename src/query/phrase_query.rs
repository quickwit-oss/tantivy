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
use postings::intersection;

pub struct PhraseQuery {
    terms: Vec<Term>,    
}

impl Query for PhraseQuery {
     
    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> io::Result<TimerTree> {
        let mut timer_tree = TimerTree::default();
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
                    while postings.next() {
                        collector.collect(postings.doc());
                    }
                }
            }
        }
        Ok(timer_tree)
    }
}

impl PhraseQuery {
    pub fn new(terms: Vec<Term>) -> PhraseQuery {
        PhraseQuery {
            terms: terms,
        }
    }
    
    fn search_segment<'a, 'b>(&'b self, reader: &'b SegmentReader, mut timer: OpenTimer<'a>) -> Box<Postings + 'b> {
        if self.terms.len() == 1 {
            match reader.get_term(&self.terms[0]) {
                Some(term_info) => {
                    let postings: SegmentPostings<'b> = reader.read_postings(&term_info);
                    Box::new(postings)
                },
                None => {
                    Box::new(SegmentPostings::empty())
                },
            }
        } else {
            let mut segment_postings: Vec<SegmentPostings> = Vec::new();
            {
                let mut decode_timer = timer.open("decode_all");
                for term in self.terms.iter() {
                    match reader.get_term(term) {
                        Some(term_info) => {
                            let _decode_one_timer = decode_timer.open("decode_one");
                            let segment_posting = reader.read_postings_with_positions(&term_info);
                            segment_postings.push(segment_posting);
                        }
                        None => {
                            // currently this is a strict intersection.
                            return Box::new(SegmentPostings::empty());
                        }
                    }
                }
            }
            Box::new(intersection(segment_postings))
        }
    }
}




