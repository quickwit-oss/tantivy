use query::Scorer;
use DocId;
use postings::DocSet;

pub struct BooleanScorer {
}


impl DocSet for BooleanScorer {
    fn advance(&mut self,) -> bool {
        panic!("a");
    }   
            
    fn doc(&self,) -> DocId {
        panic!("a");
    }
}

impl Scorer for BooleanScorer {
    
    fn score(&self,) -> f32 {
        panic!("");
    }
}