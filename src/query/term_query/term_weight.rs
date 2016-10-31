use Term;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use query::EmptyScorer;
use postings::SegmentPostingsOption;
use super::term_scorer::TermScorer;
use Result;

pub struct TermWeight {
    pub doc_freq: u32,
    pub term: Term,     
}


impl Weight for TermWeight {
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let specialized_scorer_option = try!(self.specialized_scorer(reader));
        match specialized_scorer_option {
            Some(term_scorer) => {
                Ok(box term_scorer)
            } 
            None => {
                Ok(box EmptyScorer)
            }
        }
    }
    
}

impl TermWeight {
    
    pub fn specialized_scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Option<TermScorer<'a>>> {
        let field = self.term.field();
        let fieldnorm_reader = try!(reader.get_fieldnorms_reader(field));
        Ok(
            reader.read_postings(&self.term, SegmentPostingsOption::Freq)
              .map(|segment_postings|
                TermScorer {
                    idf: 1f32 / (self.doc_freq as f32),
                    fieldnorm_reader: fieldnorm_reader,
                    segment_postings: segment_postings,
                }
              )
        )
    }
    
}