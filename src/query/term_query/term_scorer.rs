use Score;
use DocId;
use postings::SegmentPostings;
use fastfield::U32FastFieldReader;
use postings::DocSet;
use query::Scorer;

pub struct TermScorer<'a> {
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
        1.0
    } 
}
