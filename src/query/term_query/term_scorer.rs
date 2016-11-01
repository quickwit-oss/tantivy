use Score;
use DocId;
use postings::SegmentPostings;
use fastfield::U32FastFieldReader;
use postings::DocSet;
use query::Scorer;
use postings::Postings;

pub struct TermScorer<'a> {
    pub idf: Score,
    pub fieldnorm_reader: U32FastFieldReader,
    pub segment_postings: SegmentPostings<'a>,
}

impl<'a> DocSet for TermScorer<'a> {

    fn advance(&mut self,) -> bool {
        self.segment_postings.advance()
    }
      
    fn doc(&self,) -> DocId {
        self.segment_postings.doc()
    }
}

impl<'a> Scorer for TermScorer<'a> {
    fn score(&self,) -> Score {
        let doc = self.segment_postings.doc();
        let field_norm = self.fieldnorm_reader.get(doc);
        self.idf * (self.segment_postings.term_freq() as f32 / field_norm as f32).sqrt()
    } 
}

