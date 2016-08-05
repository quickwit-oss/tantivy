use schema::Term;
use query::Query;
use common::TimerTree;
use common::OpenTimer;
use std::io;
use core::searcher::Searcher;
use collector::Collector;
use SegmentLocalId;
use core::SegmentReader;
use query::MultiTermExplainScorer;
use postings::SegmentPostings;
use postings::UnionPostings;
use postings::DocSet;
use query::TfIdfScorer;
use postings::SkipResult;
use fastfield::U32FastFieldReader;
use ScoredDoc;
use query::Scorer;
use query::MultiTermAccumulator;
use DocAddress;
use query::Explanation;

#[derive(Eq, PartialEq, Debug)]
pub struct MultiTermQuery {
    terms: Vec<Term>,    
}

impl Query for MultiTermQuery {

    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> Result<Explanation, String> {
            let segment_reader = &searcher.segments()[doc_address.segment_ord() as usize];
            let multi_term_scorer = MultiTermExplainScorer::from(self.scorer(searcher));
            let mut timer_tree = TimerTree::new();
            let mut postings = self.search_segment(
                    segment_reader,
                    multi_term_scorer,
                    timer_tree.open("explain"));
            match postings.skip_next(doc_address.doc()) {
                SkipResult::Reached => {
                    let scorer = postings.scorer();
                    let explanation = scorer.explain_score(); 
                    Ok(explanation)
                }
                _ => {
                    // TODO return some kind of Error
                    panic!("could not compute explain");
                }
            }   
    }

    fn search<C: Collector>(
        &self,
        searcher: &Searcher,
        collector: &mut C) -> io::Result<TimerTree> {
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
                        let scored_doc = ScoredDoc(postings.scorer().score(), postings.doc());
                        collector.collect(scored_doc);
                    }
                }
            }
        }
        Ok(timer_tree)
    }
}


impl MultiTermQuery {
    
    pub fn num_terms(&self,) -> usize {
        self.terms.len()
    } 
    
    fn scorer(&self, searcher: &Searcher) -> TfIdfScorer {
        let num_docs = searcher.num_docs() as f32;
        let idfs: Vec<f32> = self.terms.iter()
            .map(|term| searcher.doc_freq(term))
            .map(|doc_freq| {
                if doc_freq == 0 {
                    1.
                }
                else {
                    1. + ( num_docs / (doc_freq as f32) ).ln()
                }
            })
            .collect();
        let query_coords = (0..self.terms.len() + 1)
            .map(|i| (i as f32) / (self.terms.len() as f32))
            .collect();
        // TODO have the actual terms in these names
        let term_names = self.terms
            .iter()
            .map(|term| format!("{:?}", term))
            .collect();
        let mut tfidf_scorer = TfIdfScorer::new(query_coords, idfs);
        tfidf_scorer.set_term_names(term_names);
        tfidf_scorer
    }
    
    pub fn new(terms: Vec<Term>) -> MultiTermQuery {
        MultiTermQuery {
            terms: terms,
        }
    }
        
    fn search_segment<'a, 'b, TScorer: MultiTermAccumulator>(&'b self, reader: &'b SegmentReader, multi_term_scorer: TScorer, mut timer: OpenTimer<'a>) -> UnionPostings<SegmentPostings, TScorer> {
        let mut segment_postings: Vec<SegmentPostings> = Vec::with_capacity(self.terms.len());
        let mut fieldnorms_readers: Vec<U32FastFieldReader> = Vec::with_capacity(self.terms.len());
        {
            let mut decode_timer = timer.open("decode_all");
            for term in &self.terms {
                let _decode_one_timer = decode_timer.open("decode_one");
                reader.read_postings(term)
                      .map(|postings| {
                        let field = term.get_field();
                        fieldnorms_readers.push(reader.get_fieldnorms_reader(field).unwrap());
                        segment_postings.push(postings);
                      });
            }
        }
        UnionPostings::new(fieldnorms_readers, segment_postings, multi_term_scorer)
    }
}
