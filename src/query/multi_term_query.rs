use Result;
use super::Weight;
use std::any::Any;
use Error;
use schema::Term;
use query::Query;
use core::searcher::Searcher;
use core::SegmentReader;
use query::TfIdf;
use query::Scorer;
use query::occur::Occur;
use postings::SegmentPostingsOption;
use query::DAATMultiTermScorer;



struct MultiTermWeight {
    query: MultiTermQuery,
    similitude: TfIdf,
}


impl Weight for MultiTermWeight {
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        
        
        let mut postings_and_fieldnorms = Vec::with_capacity(self.query.num_terms());
        {
            for &(occur, ref term) in &self.query.occur_terms {
                if let Some(postings) = reader.read_postings(term, SegmentPostingsOption::Freq) {
                    let field = term.field();
                    let fieldnorm_reader = try!(reader.get_fieldnorms_reader(field));
                    postings_and_fieldnorms.push((occur, postings, fieldnorm_reader));
                }
            }
        }
        if postings_and_fieldnorms.len() > 64 {
            // TODO putting the SHOULD at the end of the list should push the limit.
            return Err(Error::InvalidArgument(String::from("Limit of 64 terms was exceeded.")));
        }
        Ok(box DAATMultiTermScorer::new(postings_and_fieldnorms, self.similitude.clone()))
    }
}

/// Query involving one or more terms.
#[derive(Eq, Clone, PartialEq, Debug)]
pub struct MultiTermQuery {
    occur_terms: Vec<(Occur, Term)>,    
}

impl MultiTermQuery {
    
    /// Accessor for the number of terms
    pub fn num_terms(&self,) -> usize {
        self.occur_terms.len()
    }
    
    /// Builds the similitude object
    fn similitude(&self, searcher: &Searcher) -> TfIdf {
        let num_terms = self.num_terms();
        let num_docs = searcher.num_docs() as f32;
        let idfs: Vec<f32> = self.occur_terms
            .iter()
            .map(|&(_, ref term)| searcher.doc_freq(term))
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
    
    fn as_any(&self) -> &Any {
        self
    }
    
    fn weight(&self, searcher: &Searcher) -> Result<Box<Weight>> {
        let similitude = self.similitude(searcher);
        Ok(
            Box::new(MultiTermWeight {
                query: self.clone(),
                similitude: similitude
            })
        )
    }

}

