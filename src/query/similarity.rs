use Score;
use query::Explanation;
use query::MultiTermAccumulator;

pub trait Similarity: MultiTermAccumulator {
    fn score(&self, ) -> Score;
    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation;
}