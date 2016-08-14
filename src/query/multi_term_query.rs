use Result;
use Error;
use schema::Term;
use query::Query;
use common::TimerTree;
use common::OpenTimer;
use core::searcher::Searcher;
use collector::Collector;
use SegmentLocalId;
use core::SegmentReader;
use query::SimilarityExplainer;
use postings::SegmentPostings;
use postings::DocSet;
use query::TfIdf;
use postings::SkipResult;
use ScoredDoc;
use query::Scorer;
use query::MultiTermAccumulator;
use DocAddress;
use query::Explanation;
use query::occur::Occur;
use postings::SegmentPostingsOption;
use query::DAATMultiTermScorer;


#[derive(Eq, PartialEq, Debug)]
pub struct MultiTermQuery {
    occur_terms: Vec<(Occur, Term)>,    
}


impl MultiTermQuery {
    
    pub fn num_terms(&self,) -> usize {
        self.occur_terms.len()
    } 
    
    fn similitude(&self, searcher: &Searcher) -> TfIdf {
        let num_terms = self.num_terms();
        let num_docs = searcher.num_docs() as f32;
        let idfs: Vec<f32> = self.occur_terms
            .iter()
            .map(|&(_, ref term)| searcher.doc_freq(&term))
            .map(|doc_freq| {
                if doc_freq == 0 {
                    1.
                }
                else {
                    1. + ( num_docs / (doc_freq as f32) ).ln()
                }
            })
            .collect();
        let query_coords = (0..num_terms + 1)
            .map(|i| (i as f32) / (num_terms as f32))
            .collect();
        // TODO have the actual terms in these names
        let term_names = self.occur_terms
            .iter()
            .map(|&(_, ref term)| format!("{:?}", &term))
            .collect();
        let mut tfidf = TfIdf::new(query_coords, idfs);
        tfidf.set_term_names(term_names);
        tfidf
    }
      
    fn search_segment<'a, 'b, TAccumulator: MultiTermAccumulator>(
            &'b self,
            reader: &'b SegmentReader,
            accumulator: TAccumulator,
            mut timer: OpenTimer<'a>) -> Result<DAATMultiTermScorer<SegmentPostings, TAccumulator>> {
        let mut postings_and_fieldnorms = Vec::with_capacity(self.num_terms());
        {
            let mut decode_timer = timer.open("decode_all");
            for &(occur, ref term) in &self.occur_terms {
                let _decode_one_timer = decode_timer.open("decode_one");
                match reader.read_postings(&term, SegmentPostingsOption::Freq) {
                    Some(postings) => {
                        let field = term.get_field();
                        let fieldnorm_reader = try!(reader.get_fieldnorms_reader(field));
                        postings_and_fieldnorms.push((occur, postings, fieldnorm_reader));
                    }
                    None => {}
                }
            }
        }
        if postings_and_fieldnorms.len() > 64 {
            // TODO putting the SHOULD at the end of the list should push the limit.
            return Err(Error::InvalidArgument(String::from("Limit of 64 terms was exceeded.")));
        }
        Ok(DAATMultiTermScorer::new(postings_and_fieldnorms, accumulator))
    }
}


impl From<Vec<(Occur, Term)>> for MultiTermQuery {
    fn from(occur_terms: Vec<(Occur, Term)>) -> MultiTermQuery {
        MultiTermQuery {
            occur_terms: occur_terms,
        }
    }
}

impl From<Vec<Term>> for MultiTermQuery {
    fn from(terms: Vec<Term>) -> MultiTermQuery {
        let should_terms = terms
            .into_iter()
            .map(|term| (Occur::Should, term))
            .collect();
        MultiTermQuery {
            occur_terms: should_terms,
        }
    }
}

impl Query for MultiTermQuery {

    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> Result<Explanation> {
            let segment_reader = &searcher.segments()[doc_address.segment_ord() as usize];
            let similitude = SimilarityExplainer::from(self.similitude(searcher));
            let mut timer_tree = TimerTree::new();
            let mut postings = try!(
                self.search_segment(
                    segment_reader,
                    similitude,
                    timer_tree.open("explain"))
            );
            Ok(match postings.skip_next(doc_address.doc()) {
                SkipResult::Reached => {
                    let scorer = postings.scorer();
                    scorer.explain_score()
                }
                _ => {
                    let mut explanation = Explanation::with_val(0f32);
                    explanation.description(&format!("Failed to run explain: the document {:?} does not match", doc_address));
                    explanation
                }
            }) 
    }

    fn search<C: Collector>(
        &self,
        searcher: &Searcher,
        collector: &mut C) -> Result<TimerTree> {
        let mut timer_tree = TimerTree::new();        
        {
            let mut search_timer = timer_tree.open("search");
            for (segment_ord, segment_reader) in searcher.segments().iter().enumerate() {
                let mut segment_search_timer = search_timer.open("segment_search");
                {
                    let _ = segment_search_timer.open("set_segment");
                    try!(collector.set_segment(segment_ord as SegmentLocalId, &segment_reader));
                }
                let mut postings = try!(
                    self.search_segment(
                        segment_reader,
                        self.similitude(searcher),
                        segment_search_timer.open("get_postings"))
                );
                {
                    let _collection_timer = segment_search_timer.open("collection");
                    while postings.advance() {
                        let scored_doc = ScoredDoc(postings.score(), postings.doc());
                        collector.collect(scored_doc);
                    }
                }
            }
        }
        Ok(timer_tree)
    }
}

