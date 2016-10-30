use query::Scorer;
use DocSet;
use Score;
use DocId;

pub struct EmptyScorer;

impl Scorer for EmptyScorer {
    fn score(&self) -> Score {
        0f32
    }
}

impl DocSet for EmptyScorer {
    fn advance(&mut self) -> bool {
        false
    }

    fn doc(&self) -> DocId {
        0
    }
}
