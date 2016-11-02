use query::Scorer;
use DocSet;
use query::term_query::TermScorer;
use query::boolean_query::BooleanScorer;
use postings::SegmentPostings;
use postings::Postings;
use DocId;

pub struct PhraseScorer<'a> {
    pub all_term_scorer: BooleanScorer<TermScorer<SegmentPostings<'a>>>
}

impl<'a> PhraseScorer<'a> {
    fn phrase_match(&self) -> bool {
        let scorers = self.all_term_scorer.scorers();
        for scorer in scorers {
            let positions = scorer.postings().positions();
        }
        true
        // self.all_term_scorer.positions();
        // let positions = 

    }
}

impl<'a> DocSet for PhraseScorer<'a> {
    fn advance(&mut self,) -> bool {
        while self.all_term_scorer.advance() {
            if self.phrase_match() {
                return true;
            }
        }
        false
    }

    fn doc(&self,) -> DocId {
        self.all_term_scorer.doc()
    }
}


impl<'a> Scorer for PhraseScorer<'a> {
    fn score(&self,) -> f32 {
        self.all_term_scorer.score()
    }
    
}
