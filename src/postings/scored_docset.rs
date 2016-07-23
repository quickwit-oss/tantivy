use postings::DocSet;

pub trait ScoredDocSet: DocSet {
    fn score(&self,) -> f32;
}