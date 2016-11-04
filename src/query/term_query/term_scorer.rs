use Score;
use DocId;
use fastfield::U32FastFieldReader;
use postings::DocSet;
use query::Scorer;
use postings::Postings;

pub struct TermScorer<TPostings> where TPostings: Postings {
    pub idf: Score,
    pub fieldnorm_reader: U32FastFieldReader,
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
        let doc = self.postings.doc();
        let field_norm = self.fieldnorm_reader.get(doc);
        self.idf * (self.postings.term_freq() as f32 / field_norm as f32).sqrt()
    } 
}

