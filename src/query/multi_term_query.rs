use schema::Term;
use query::Query;
use common::TimerTree;
use common::OpenTimer;
use std::io;
use core::searcher::Searcher;
use collector::Collector;
use core::searcher::SegmentLocalId;
use core::SegmentReader;
use postings::SegmentPostings;
use postings::UnionPostings;
use postings::ScoredDocSet;
use postings::DocSet;
use query::MultiTermScorer;
use std::iter;


pub struct MultiTermQuery {
    terms: Vec<Term>,    
}

impl Query for MultiTermQuery {

    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> io::Result<TimerTree> {
        let mut timer_tree = TimerTree::new();
        
        let multi_term_scorer = self.scorer(searcher);
        {
            let mut search_timer = timer_tree.open("search");
            for (segment_ord, segment_reader) in searcher.segments().iter().enumerate() {
                let mut segment_search_timer = search_timer.open("segment_search");
                {
                    let _ = segment_search_timer.open("set_segment");
                    try!(collector.set_segment(segment_ord as SegmentLocalId, &segment_reader));
                }
                let mut postings = self.search_segment(
                        segment_reader,
                        multi_term_scorer.clone(),
                        segment_search_timer.open("get_postings"));
                {
                    let _collection_timer = segment_search_timer.open("collection");
                    while postings.next() {
                        collector.collect(postings.doc(), postings.score());
                    }
                }
            }
        }
        Ok(timer_tree)
    }
}

impl MultiTermQuery {
    
    fn scorer(&self, searcher: &Searcher) -> MultiTermScorer {
        let idfs: Vec<f32> = self.terms.iter()
            .map(|term| searcher.doc_freq(term))
            .map(|doc_freq| {
                if doc_freq == 0 {
                    return 1.
                }
                else {
                    1.0 / (doc_freq as f32)
                }
            })
            .collect();
        let query_coord = iter::repeat(1f32).take(self.terms.len()).collect();
        MultiTermScorer::new(query_coord, idfs)
    }

    pub fn new(terms: Vec<Term>) -> MultiTermQuery {
        MultiTermQuery {
            terms: terms,
        }
    }
        
    fn search_segment<'a, 'b>(&'b self, reader: &'b SegmentReader, multi_term_scorer: MultiTermScorer, mut timer: OpenTimer<'a>) -> UnionPostings<SegmentPostings> {
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
                        segment_postings.push(SegmentPostings::empty());
                    }
                }
            }
        }
        UnionPostings::new(segment_postings, multi_term_scorer)
    }
}
