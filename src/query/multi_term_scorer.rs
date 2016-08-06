use query::Scorer;
use query::Explanation;

pub trait MultiTermAccumulator {
    fn update(&mut self, term_ord: usize, term_freq: u32, fieldnorm: u32);
    fn clear(&mut self,);
}

pub trait MultiTermScorer: Scorer + MultiTermAccumulator {
    fn explain(&self, vals: &Vec<(usize, u32, u32)>) -> Explanation;
}
