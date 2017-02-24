use Score;
use DocId;
use fastfield::U32FastFieldReader;
use postings::DocSet;
use query::Scorer;
use postings::Postings;

pub struct TermScorer<TPostings> where TPostings: Postings {
    pub postings: TPostings,
}

impl<TPostings> TermScorer<TPostings> where TPostings: Postings {
    pub fn postings(&self) -> &TPostings {
        &self.postings
    }
}

impl<TPostings> DocSet for TermScorer<TPostings> where TPostings: Postings {
    fn advance(&mut self,) -> bool {
        self.postings.advance()
    }
      
    fn doc(&self,) -> DocId {
        self.postings.doc()
    }
}

impl<TPostings> Scorer for TermScorer<TPostings> where TPostings: Postings {
    fn score(&self,) -> Score {
        1.0
    } 
}

